#!/usr/bin/env python3
"""
NBA Betting Web Application - Quick Start

Usage:
    python start_app.py                    # Start on localhost:5000
    python start_app.py --port 8080        # Custom port
    python start_app.py --debug            # Enable debug mode (off by default)

Requirements:
    pip install -e ".[web]"                # Install web dependencies
    cp .env.example .env                   # Create environment file
    # Edit .env with your settings
"""

import argparse
import os
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Start the NBA Betting web application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_app.py                  Start on http://localhost:5000
  python start_app.py --port 8080      Start on http://localhost:8080
  python start_app.py --debug          Enable Flask debug mode
  python start_app.py --public         Allow external connections (0.0.0.0)
        """,
    )
    parser.add_argument(
        "--port", "-p", type=int, default=5000, help="Port to run on (default: 5000)"
    )
    parser.add_argument(
        "--public", action="store_true", help="Allow external connections (bind to 0.0.0.0)"
    )
    parser.add_argument(
        "--debug", "-d", action="store_true", default=False, help="Enable Flask debug mode (off by default for security)"
    )
    parser.add_argument(
        "--no-debug", action="store_true", help="Disable Flask debug mode"
    )
    args = parser.parse_args()

    # --no-debug overrides --debug
    debug_mode = args.debug and not args.no_debug
    host = "0.0.0.0" if args.public else "127.0.0.1"

    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("Warning: python-dotenv not installed. Environment variables must be set manually.")

    # Print startup banner
    print("\n" + "=" * 60)
    print("  NBA Betting Web Application")
    print("=" * 60)
    print(f"\n  URL: http://{host}:{args.port}")
    print(f"  Debug: {'ON' if debug_mode else 'OFF'}")
    print("\n  Press Ctrl+C to stop")
    print("=" * 60 + "\n")

    # Import and run the app
    try:
        from src.app.web import app
        app.run(host=host, port=args.port, debug=debug_mode)
    except ImportError as e:
        print(f"\nError importing web app: {e}")
        print("\nMake sure web dependencies are installed:")
        print("  pip install -e '.[web]'")
        sys.exit(1)


if __name__ == "__main__":
    main()
