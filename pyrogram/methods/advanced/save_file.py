#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import functools
import inspect
import io
import logging
import math
import os
from hashlib import md5
from pathlib import PurePath
from typing import Union, Callable

import pyrogram
from pyrogram import StopTransmission
from pyrogram import raw
from pyrogram.errors import FloodWait, FloodPremiumWait
from pyrogram.session import Session

log = logging.getLogger(__name__)

# MTProto hard limit for a single file part.
PART_SIZE = 512 * 1024

# Concurrency bounds for the parallel upload pipeline. Small files don't
# benefit from (and shouldn't pay the connection-setup cost of) a large
# worker pool; big files get scaled up to spread parts across multiple
# DC connections and saturate available bandwidth (helps Premium sessions,
# which permit more concurrent uploads per DC).
MIN_WORKERS_BIG = 8
MAX_WORKERS_BIG = 16
MAX_SESSIONS_PER_UPLOAD = 4


class SaveFile:
    async def save_file(
        self: "pyrogram.Client",
        path: Union[str, "io.BytesIO"],
        file_id: int = None,
        file_part: int = 0,
        progress: Callable = None,
        progress_args: tuple = ()
    ):
        """Upload a file onto Telegram servers, without actually sending the message to anyone.
        Useful whenever an InputFile type is required.

        .. note::

            This is a utility method intended to be used **only** when working with raw
            :obj:`functions <pyrogram.raw.functions>` (i.e: a Telegram API method you wish to use which is not
            available yet in the Client class as an easy-to-use method).

        Parameters:
            path (``str`` | :obj:`io.BytesIO`):
                The path of the file you want to upload that exists on your local machine or a binary file-like object
                with its attribute ".name" set for in-memory uploads.

            file_id (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            file_part (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            progress (``Callable``, *optional*):
                Pass a callback function to view the file transmission progress.
                The function must take *(current, total)* as positional arguments (look at Other Parameters below for a
                detailed description) and will be called back each time a new file chunk has been successfully
                transmitted.

            progress_args (``tuple``, *optional*):
                Extra custom arguments for the progress callback function.
                You can pass anything you need to be available in the progress callback scope; for example, a Message
                object or a Client instance in order to edit the message with the updated progress status.

        Other Parameters:
            current (``int``):
                The amount of bytes transmitted so far.

            total (``int``):
                The total size of the file.

            *args (``tuple``, *optional*):
                Extra custom arguments as defined in the ``progress_args`` parameter.
                You can either keep ``*args`` or add every single extra argument in your function signature.

        Returns:
            :obj:`~pyrogram.raw.base.InputFile`: On success, the uploaded file is returned in form of an InputFile object.

        Raises:
            :obj:`~pyrogram.errors.RPCError`: In case of a Telegram RPC error.

        """
        if path is None:
            return None

        part_size = PART_SIZE

        if isinstance(path, (str, PurePath)):
            fp = open(path, "rb")
        elif isinstance(path, io.IOBase):
            fp = path
        else:
            raise ValueError("Invalid file. Expected a file path as string or a binary (not text) file pointer")

        file_name = getattr(fp, "name", "file.jpg")

        fp.seek(0, os.SEEK_END)
        file_size = fp.tell()
        fp.seek(0)

        if file_size == 0:
            raise ValueError("File size equals to 0 B")

        # TODO
        file_size_limit_mib = 2000
        if self.me and self.me.is_premium:
            file_size_limit_mib = 4000

        if file_size > file_size_limit_mib * 1024 * 1024:
            raise ValueError(f"Can't upload files bigger than {file_size_limit_mib} MiB")

        file_total_parts = int(math.ceil(file_size / part_size))
        is_big = file_size > 10 * 1024 * 1024
        is_missing_part = file_id is not None
        file_id = file_id or self.rnd_id()
        md5_sum = md5() if not is_big and not is_missing_part else None

        # --- concurrency sizing --------------------------------------------
        if is_big:
            total_workers = min(MAX_WORKERS_BIG, max(MIN_WORKERS_BIG, file_total_parts))
            pool_size = min(MAX_SESSIONS_PER_UPLOAD, max(1, total_workers // 4))
        else:
            total_workers = min(4, max(1, file_total_parts))
            pool_size = 1

        workers_per_session = max(1, math.ceil(total_workers / pool_size))

        # Bounds simultaneous in-flight RPCs across the whole pool. Each
        # worker only ever holds one part in flight at a time, so this is a
        # belt-and-suspenders cap layered on top of the worker-task count
        # (useful if workers_per_session is ever raised independently).
        semaphore = asyncio.Semaphore(total_workers)
        queue: asyncio.Queue = asyncio.Queue(total_workers * 2)
        progress_lock = asyncio.Lock()
        completed_parts = 0
        abort_event = asyncio.Event()
        first_error = []

        async def send_part(session, rpc):
            nonlocal completed_parts

            async with semaphore:
                attempts = 0
                while True:
                    try:
                        await session.invoke(rpc)
                        break
                    except (FloodWait, FloodPremiumWait) as e:
                        # Session.invoke() already retries FloodWait internally
                        # up to its own sleep_threshold, but a wait longer than
                        # that still bubbles up here. Sleep exactly the required
                        # amount and retry only this part -- never fail sibling
                        # workers or drop the rest of the stream.
                        log.warning(
                            f"[{self.name}] FloodWait: sleeping {e.value}s "
                            f"for part {getattr(rpc, 'file_part', '?')}"
                        )
                        await asyncio.sleep(e.value)
                    except StopTransmission:
                        raise
                    except Exception as e:
                        attempts += 1
                        if attempts >= 3:
                            log.error(
                                f"Giving up on part {getattr(rpc, 'file_part', '?')} "
                                f"after {attempts} attempts: {e}"
                            )
                            first_error.append(e)
                            abort_event.set()
                            return
                        log.warning(f"Retrying part after error ({attempts}/3): {e}")
                        await asyncio.sleep(attempts)

            if progress:
                async with progress_lock:
                    completed_parts += 1
                    current = min(completed_parts * part_size, file_size)

                func = functools.partial(progress, current, file_size, *progress_args)

                if inspect.iscoroutinefunction(progress):
                    await func()
                else:
                    await self.loop.run_in_executor(self.executor, func)

        async def worker(session):
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    await send_part(session, item)
                finally:
                    queue.task_done()

        pool = [
            Session(
                self, await self.storage.dc_id(), await self.storage.auth_key(),
                await self.storage.test_mode(), is_media=True
            ) for _ in range(pool_size)
        ]

        workers = [
            self.loop.create_task(worker(session))
            for session in pool
            for _ in range(workers_per_session)
        ]

        try:
            for session in pool:
                await session.start()

            fp.seek(part_size * file_part)

            while True:
                if abort_event.is_set():
                    raise first_error[0] if first_error else RuntimeError("Upload aborted: a file part failed")

                chunk = fp.read(part_size)

                if not chunk:
                    if not is_big and not is_missing_part:
                        md5_sum = "".join([hex(i)[2:].zfill(2) for i in md5_sum.digest()])
                    break

                if is_big:
                    rpc = raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=file_part,
                        file_total_parts=file_total_parts,
                        bytes=chunk
                    )
                else:
                    rpc = raw.functions.upload.SaveFilePart(
                        file_id=file_id,
                        file_part=file_part,
                        bytes=chunk
                    )

                await queue.put(rpc)

                if is_missing_part:
                    return

                if not is_big and not is_missing_part:
                    md5_sum.update(chunk)

                file_part += 1

            # Block until every queued part has actually been acknowledged
            # by Telegram (not just handed off), so callers never receive
            # an InputFile referencing parts still in flight.
            await queue.join()

            if abort_event.is_set():
                raise first_error[0] if first_error else RuntimeError("Upload aborted: a file part failed")
        except StopTransmission:
            raise
        except Exception as e:
            log.error(e, exc_info=True)
            raise
        else:
            if is_big:
                return raw.types.InputFileBig(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,

                )
            else:
                return raw.types.InputFile(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,
                    md5_checksum=md5_sum
                )
        finally:
            for _ in workers:
                await queue.put(None)

            await asyncio.gather(*workers, return_exceptions=True)

            for session in pool:
                await session.stop()
            if isinstance(path, (str, PurePath)):
                fp.close()
