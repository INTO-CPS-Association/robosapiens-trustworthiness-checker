from __future__ import annotations

import logging
import os
from pathlib import Path
import sys
import traceback

_DEBUG_LOG = os.environ.get("TC_UNIFMU_DEBUG_LOG")


def _debug(message: str) -> None:
    if _DEBUG_LOG is None:
        return
    with Path(_DEBUG_LOG).open("a", encoding="utf-8") as log:
        log.write(message)
        log.write("\n")


_debug(f"starting backend with executable={sys.executable!r} argv={sys.argv!r} cwd={os.getcwd()!r}")

_RESOURCES = Path(__file__).resolve().parent
_SITE_PACKAGES = _RESOURCES / "site-packages"
if _SITE_PACKAGES.exists():
    sys.path.insert(0, str(_SITE_PACKAGES))

try:
    import trustworthiness_checker
    _debug("imported trustworthiness_checker")
    from backend import Backend
    _debug("imported backend")
except Exception:
    _debug(traceback.format_exc())
    raise

del trustworthiness_checker

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)


if __name__ == "__main__":
    dispatcher_endpoint = os.environ["UNIFMU_DISPATCHER_ENDPOINT"]
    logger.info("dispatcher endpoint received: %s", dispatcher_endpoint)
    _debug(f"dispatcher endpoint received: {dispatcher_endpoint!r}")

    backend = Backend()
    _debug("constructed backend")
    backend.connect_to_endpoint(dispatcher_endpoint)
    _debug("connected to dispatcher")
    backend.handshake()
    _debug("handshake sent")
    backend.command_reply_loop()
