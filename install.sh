#!/usr/bin/env bash
#
# install.sh — Install mysqlpg & mysqldumppg
#
# Usage:
#   ./install.sh              # Install + add aliases
#   ./install.sh --no-alias   # Install without aliases
#   ./install.sh --uninstall  # Remove package and aliases
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALIAS_MARKER="# mysqlpg aliases"
SHELL_RC=""

# Detect shell rc file
detect_shell_rc() {
    if [ -n "${ZSH_VERSION:-}" ] || [ "$(basename "${SHELL:-}")" = "zsh" ]; then
        SHELL_RC="$HOME/.zshrc"
    else
        SHELL_RC="$HOME/.bashrc"
    fi
}

# Add aliases to shell rc
add_aliases() {
    detect_shell_rc

    if grep -qF "$ALIAS_MARKER" "$SHELL_RC" 2>/dev/null; then
        echo "Aliases already present in $SHELL_RC"
        return 0
    fi

    cat >> "$SHELL_RC" << 'ALIASES'

# mysqlpg aliases — MySQL-compatible CLI for PostgreSQL
alias mysql='mysqlpg'
alias mysqldump='mysqldumppg'
ALIASES

    echo "Added mysql/mysqldump aliases to $SHELL_RC"
    echo "Run 'source $SHELL_RC' or restart your shell to activate."
}

# Remove aliases from shell rc
remove_aliases() {
    detect_shell_rc

    if [ ! -f "$SHELL_RC" ]; then
        return 0
    fi

    # Remove the alias block (marker line + 3 lines after)
    if grep -qF "$ALIAS_MARKER" "$SHELL_RC" 2>/dev/null; then
        # Use sed to remove the block: blank line before, marker, and two alias lines
        sed -i '/^$/N;/\n# mysqlpg aliases/{N;N;d;}' "$SHELL_RC"
        echo "Removed aliases from $SHELL_RC"
    fi
}

# Install the package
install() {
    echo "Installing mysqlpg..."

    # Check Python
    if ! command -v python3 &>/dev/null && ! command -v python &>/dev/null; then
        echo "ERROR: Python 3 is required but not found."
        exit 1
    fi

    PYTHON="$(command -v python3 || command -v python)"

    # Check pip
    if ! "$PYTHON" -m pip --version &>/dev/null; then
        echo "ERROR: pip is required. Install with: $PYTHON -m ensurepip"
        exit 1
    fi

    # Install in development mode (editable) or regular mode
    if [ -f "$SCRIPT_DIR/pyproject.toml" ]; then
        echo "Installing from source at $SCRIPT_DIR..."
        "$PYTHON" -m pip install -e "$SCRIPT_DIR" --quiet
    else
        echo "ERROR: pyproject.toml not found in $SCRIPT_DIR"
        exit 1
    fi

    echo ""
    echo "Installed successfully!"
    echo "  mysqlpg    — MySQL-compatible CLI for PostgreSQL"
    echo "  mysqldumppg — mysqldump-compatible dump tool for PostgreSQL"
    echo ""

    # Verify installation
    if command -v mysqlpg &>/dev/null; then
        echo "Version: $(mysqlpg -V)"
    fi
}

# Uninstall
uninstall() {
    echo "Uninstalling mysqlpg..."
    PYTHON="$(command -v python3 || command -v python)"
    "$PYTHON" -m pip uninstall mysqlpg -y 2>/dev/null || true
    remove_aliases
    echo "Uninstalled."
}

# Main
case "${1:-}" in
    --no-alias)
        install
        ;;
    --uninstall)
        uninstall
        ;;
    --alias-only)
        add_aliases
        ;;
    --help|-h)
        echo "Usage: $0 [--no-alias|--uninstall|--alias-only|--help]"
        echo ""
        echo "  (default)      Install mysqlpg and add mysql/mysqldump aliases"
        echo "  --no-alias     Install without adding shell aliases"
        echo "  --alias-only   Only add aliases (skip pip install)"
        echo "  --uninstall    Remove package and aliases"
        echo "  --help         Show this help"
        ;;
    *)
        install
        echo ""
        read -p "Add 'mysql' and 'mysqldump' aliases to your shell? [Y/n] " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            add_aliases
        else
            echo "Skipping aliases. You can add them later with: $0 --alias-only"
        fi
        ;;
esac
