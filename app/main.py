from __future__ import annotations

import uvicorn

from .api import create_app
from .config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "app.api:app",
        host=settings.app.host,
        port=settings.app.port,
        log_level=settings.app.log_level,
        reload=False,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
