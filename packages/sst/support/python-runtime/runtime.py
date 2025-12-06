import os
import sys
import json
import logging
import argparse
import traceback
import signal
from urllib import request
from time import time
from importlib import import_module

class Identity(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    #__slots__ = ["cognito_identity_id", "cognito_identity_pool_id"]

class ClientContext(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    #__slots__ = ['custom', 'env', 'client']

class Context(object):
    def __init__(self, invoked_function_arn, aws_request_id, deadline_ms, identity, client_context, log_group_name, log_stream_name):
        self.function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'local')
        self.invoked_function_arn = invoked_function_arn
        self.aws_request_id = aws_request_id
        self.memory_limit_in_mb = os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', '128')
        self.deadline_ms = deadline_ms
        # If identity is null, we want to mimick AWS behavior and return an object with None values
        self.identity = Identity(**json.loads(identity)) if identity and identity != 'null' else Identity(cognito_identity_id=None, cognito_identity_pool_id=None)
        # If client_context is null, we want to mimick AWS behavior and return None
        self.client_context = ClientContext(**json.loads(client_context)) if client_context and client_context != 'null' else None
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name

    def get_remaining_time_in_millis(self):
        return int(max(self.deadline_ms - int(round(time() * 1000)), 0))

    def log(self):
        return sys.stdout.write


def handleUnserializable(obj):
    raise TypeError("Unserializable {}: {!r}".format(type(obj), obj))


# Idle timeout handler
class IdleTimeoutError(Exception):
    pass

def idle_timeout_handler(signum, frame):
    raise IdleTimeoutError("Worker idle timeout")


logging.basicConfig()

parser = argparse.ArgumentParser(
    prog='invoke',
    description='Runs a Lambda entry point (handler) with an optional event',
)

parser.add_argument('handler_module',
                    help=('Module containing the handler function,'
                          ' omitting ".py". IE: "path.to.module"'))
parser.add_argument('src_path', help='SrcPath of the handler function')
parser.add_argument('handler_name', help='Name of the handler function')

# Idle timeout in seconds (15 minutes, matching Node.js runtime)
IDLE_TIMEOUT = 15 * 60

if __name__ == '__main__':
    args = parser.parse_args()

    # this is needed because you need to import from where you've executed sst
    sys.path.append('.')

    # set the sys.path to the src_path. Otherwise importing a local file
    # would fail with error ModuleNotFoundError
    sys.path.append(args.src_path)

    # Import handler module once at startup (warm start optimization)
    handler = None
    try:
        # remove leading zeros for relative imports
        if args.handler_module.startswith('.'):
            module = import_module(args.handler_module[1:])
        else:
            module = import_module(args.handler_module)

        handler = getattr(module, args.handler_name)
    except Exception as e:
        # Report init error and exit
        traceback.print_exc()
        try:
            init_error_url = "http://{}/2018-06-01/runtime/init/error".format(
                os.environ['AWS_LAMBDA_RUNTIME_API']
            )
            ex_type, ex_value, ex_traceback = sys.exc_info()
            error_data = json.dumps({
                "errorType": ex_type.__name__ if ex_type else "ImportError",
                "errorMessage": str(ex_value),
                "trace": traceback.format_tb(ex_traceback) if ex_traceback else [],
            }).encode("utf-8")
            req = request.Request(init_error_url, method="POST", data=error_data)
            req.add_header('Content-Type', 'application/json')
            request.urlopen(req)
        except Exception:
            pass
        sys.exit(1)

    # Main event loop - handle multiple invocations
    while True:
        context = None

        try:
            # Set up idle timeout using SIGALRM (Unix only)
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, idle_timeout_handler)
                signal.alarm(IDLE_TIMEOUT)

            # Fetch next invocation (blocks until one is available)
            next_url = "http://{}/2018-06-01/runtime/invocation/next".format(
                os.environ['AWS_LAMBDA_RUNTIME_API']
            )
            r = request.urlopen(next_url)

            # Cancel idle timeout once we have work
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)

            event = json.loads(r.read())
            context = Context(
                r.getheader('Lambda-Runtime-Invoked-Function-Arn'),
                r.getheader('Lambda-Runtime-Aws-Request-Id'),
                r.getheader('Lambda-Runtime-Deadline-Ms'),
                r.getheader('Lambda-Runtime-Cognito-Identity'),
                r.getheader('Lambda-Runtime-Client-Context'),
                r.getheader('Lambda-Runtime-Log-Group-Name'),
                r.getheader('Lambda-Runtime-Log-Stream-Name')
            )

            # Invoke handler
            try:
                result = handler(event, context)
                data = json.dumps(result, default=handleUnserializable).encode("utf-8")
                url_destination = '/response'
            except Exception as e:
                # Handler error - report but keep worker alive
                traceback.print_exc()
                ex_type, ex_value, ex_traceback = sys.exc_info()
                error_result = {
                    "errorType": ex_type.__name__ if ex_type else "Error",
                    "errorMessage": str(ex_value),
                    "trace": traceback.format_tb(ex_traceback) if ex_traceback else [],
                }
                data = json.dumps(error_result).encode("utf-8")
                url_destination = '/error'

            # Send response
            response_url = "http://{}/2018-06-01/runtime/invocation/{}{}".format(
                os.environ['AWS_LAMBDA_RUNTIME_API'],
                context.aws_request_id,
                url_destination
            )
            req = request.Request(response_url, method="POST", data=data)
            req.add_header('Content-Type', 'application/json')

            # Retry sending response (matching Node.js behavior)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    request.urlopen(req)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time as time_module
                        time_module.sleep(0.5)
                    else:
                        print(f"Failed to send response after {max_retries} attempts: {e}", file=sys.stderr)

        except IdleTimeoutError:
            # Idle timeout - exit gracefully
            print("Worker idle timeout, exiting", file=sys.stderr)
            sys.exit(0)

        except KeyboardInterrupt:
            # Graceful shutdown
            print("Worker interrupted, exiting", file=sys.stderr)
            sys.exit(0)

        except Exception as e:
            # Unexpected error in the runtime loop itself
            print(f"Runtime error: {e}", file=sys.stderr)
            traceback.print_exc()
            # Continue to next iteration - don't crash the worker
            continue
