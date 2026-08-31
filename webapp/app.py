"""Entry point Flask: bootstrap della config, avvio scheduler, dashboard."""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from flask import Flask, render_template  # noqa: E402

from scheduler.job import start_scheduler  # noqa: E402
from src.pipeline.meta_bootstrap import bootstrap  # noqa: E402
from webapp.api import api_bp  # noqa: E402


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(api_bp)

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    return app


app = create_app()

if __name__ == "__main__":
    bootstrap()
    start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=False)
