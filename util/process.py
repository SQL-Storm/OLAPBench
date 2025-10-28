import selectors
import subprocess
import time

from util import logger


class Process:
    def __init__(self, command: str, env: dict = None, cwd: str = None):
        self._command = command
        self._env = env
        self._cwd = cwd

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        logger.log_verbose_process(f'Starting command `{self._command}`')
        self.process = subprocess.Popen(self._command.split(" "), env=self._env, cwd=self._cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.selector.register(self.process.stderr, selectors.EVENT_READ)

    def stop(self):
        self.process.stdin.close()
        return_code = self.process.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, self._command)
        logger.log_verbose_process(f'Stopped command `{self._command}`')

    def kill(self):
        self.process.stdin.close()
        self.process.terminate()
        logger.log_verbose_process(f'Killed command `{self._command}`')

    def write(self, text: str):
        self.read_and_discard()

        self.process.stdin.write(text.encode())
        self.process.stdin.write(b"\n")
        self.process.stdin.flush()

        logger.log_verbose_process(f'stdin:  {text}')

    def wait(self):
        while True:
            for key, _ in self.selector.select():
                data = key.fileobj.readline().decode().strip()

                if data is None:
                    return
                elif data:
                    if key.fileobj is self.process.stdout:
                        logger.log_verbose_process(f'stdout: {data}')
                    else:
                        logger.log_verbose_process_stderr(f'stderr: {data}')

                # Check if the process has terminated
                if self.process.poll() is not None:
                    return

    def readline_stderr(self):
        while True:
            for key, _ in self.selector.select():
                data = key.fileobj.readline().decode().strip()

                if data is None:
                    raise ChildProcessError("process closed")
                elif data:
                    if key.fileobj is self.process.stdout:
                        logger.log_verbose_process(f'stdout: {data}')
                    else:
                        logger.log_verbose_process_stderr(f'stderr: {data}')
                        return data

                # Check if the process has terminated
                if self.process.poll() is not None:
                    raise ChildProcessError("process closed")

    def read_and_discard(self):
        for key, _ in self.selector.select(timeout=0):
            # Check if the process has terminated
            if self.process.poll() is not None:
                raise ChildProcessError("process closed")

            data = key.fileobj.readline().decode().strip()
            if key.fileobj is self.process.stdout:
                logger.log_verbose_process(f'stdout: {data}')
            else:
                logger.log_verbose_process_stderr(f'stderr: {data}')

    def run(self) -> str:
        logger.log_verbose_process(f'Running command `{self._command}`')
        ret = subprocess.run(self._command.split(" "), env=self._env, cwd=self._cwd, capture_output=True, text=True)
        if ret.returncode:
            logger.log_verbose_process_stderr(ret.stderr)
            raise ChildProcessError(ret.stderr)

        return ret.stdout
