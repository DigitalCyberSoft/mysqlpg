#!/usr/bin/env bash
#
# install.sh — Install mysqlpg & mysqldumppg
#
# Usage:
#   ./install.sh              # Install + create mysql/mysqldump symlinks
#   ./install.sh --no-alias   # Install without symlinks
#   ./install.sh --uninstall  # Remove package and symlinks
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Determine the right bin directory for symlinks
get_bin_dir() {
    # If running as root, use system-wide path
    if [ "$(id -u)" -eq 0 ]; then
        echo "/usr/local/bin"
    else
        # User-local: prefer ~/.local/bin (standard), create if needed
        local user_bin="$HOME/.local/bin"
        mkdir -p "$user_bin"
        echo "$user_bin"
    fi
}

# Find where pip installed the entry points
find_mysqlpg_bin() {
    local bin
    bin="$(command -v mysqlpg 2>/dev/null || true)"
    if [ -z "$bin" ]; then
        # Try common pip locations
        for dir in /usr/local/bin /usr/bin "$HOME/.local/bin"; do
            if [ -x "$dir/mysqlpg" ]; then
                bin="$dir/mysqlpg"
                break
            fi
        done
    fi
    echo "$bin"
}

# Create symlinks: mysql -> mysqlpg, mysqldump -> mysqldumppg
create_symlinks() {
    local bin_dir
    bin_dir="$(get_bin_dir)"

    local mysqlpg_bin mysqldumppg_bin
    mysqlpg_bin="$(find_mysqlpg_bin)"
    mysqldumppg_bin="$(command -v mysqldumppg 2>/dev/null || true)"

    if [ -z "$mysqlpg_bin" ]; then
        echo "WARNING: mysqlpg not found in PATH. Skipping symlinks."
        return 1
    fi

    # Get the directory where pip installed the scripts
    local pip_bin_dir
    pip_bin_dir="$(dirname "$mysqlpg_bin")"

    if [ -z "$mysqldumppg_bin" ] && [ -x "$pip_bin_dir/mysqldumppg" ]; then
        mysqldumppg_bin="$pip_bin_dir/mysqldumppg"
    fi

    # Helper: check if a command exists and is NOT one of our symlinks
    is_real_binary() {
        local cmd="$1"
        local found
        found="$(command -v "$cmd" 2>/dev/null || true)"
        [ -z "$found" ] && return 1  # not found at all

        # If it's a symlink, check if it points to mysqlpg/mysqldumppg (ours)
        if [ -L "$found" ]; then
            local target
            target="$(readlink -f "$found" 2>/dev/null || readlink "$found")"
            if [[ "$target" == *mysqlpg* ]] || [[ "$target" == *mysqldumppg* ]]; then
                return 1  # it's our own symlink, not a real binary
            fi
        fi
        return 0  # real binary exists
    }

    # Create mysql symlink
    if is_real_binary mysql; then
        local found_mysql
        found_mysql="$(command -v mysql)"
        echo "WARNING: mysql already exists at $found_mysql"
        echo "  Skipping 'mysql' symlink to avoid conflict."
        echo "  Use 'mysqlpg' directly instead."
    else
        # Remove stale symlink if present
        [ -L "$bin_dir/mysql" ] && rm "$bin_dir/mysql"
        ln -s "$mysqlpg_bin" "$bin_dir/mysql"
        echo "Created symlink: $bin_dir/mysql -> $mysqlpg_bin"
    fi

    # Create mysqldump symlink
    if is_real_binary mysqldump; then
        local found_dump
        found_dump="$(command -v mysqldump)"
        echo "WARNING: mysqldump already exists at $found_dump"
        echo "  Skipping 'mysqldump' symlink to avoid conflict."
        echo "  Use 'mysqldumppg' directly instead."
    elif [ -n "$mysqldumppg_bin" ]; then
        [ -L "$bin_dir/mysqldump" ] && rm "$bin_dir/mysqldump"
        ln -s "$mysqldumppg_bin" "$bin_dir/mysqldump"
        echo "Created symlink: $bin_dir/mysqldump -> $mysqldumppg_bin"
    fi

    # Ensure user bin dir is in PATH
    if [ "$(id -u)" -ne 0 ] && [[ ":$PATH:" != *":$bin_dir:"* ]]; then
        echo ""
        echo "NOTE: $bin_dir is not in your PATH."
        echo "  Add this to your shell profile:"
        echo "    export PATH=\"$bin_dir:\$PATH\""
    fi
}

# Remove symlinks
remove_symlinks() {
    local bin_dir
    bin_dir="$(get_bin_dir)"

    for name in mysql mysqldump; do
        local target="$bin_dir/$name"
        if [ -L "$target" ]; then
            local dest
            dest="$(readlink "$target")"
            if [[ "$dest" == *mysqlpg* ]] || [[ "$dest" == *mysqldumppg* ]]; then
                rm "$target"
                echo "Removed symlink: $target"
            fi
        fi
    done

    # Also check /usr/local/bin if user is root
    if [ "$(id -u)" -eq 0 ]; then
        return
    fi
    for name in mysql mysqldump; do
        local target="/usr/local/bin/$name"
        if [ -L "$target" ]; then
            local dest
            dest="$(readlink "$target")"
            if [[ "$dest" == *mysqlpg* ]] || [[ "$dest" == *mysqldumppg* ]]; then
                rm "$target" 2>/dev/null || true
                echo "Removed symlink: $target"
            fi
        fi
    done
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

    # Install
    if [ -f "$SCRIPT_DIR/pyproject.toml" ]; then
        echo "Installing from source at $SCRIPT_DIR..."
        "$PYTHON" -m pip install -e "$SCRIPT_DIR" --quiet
    else
        echo "ERROR: pyproject.toml not found in $SCRIPT_DIR"
        exit 1
    fi

    echo ""
    echo "Installed successfully!"
    echo "  mysqlpg     — MySQL-compatible CLI for PostgreSQL"
    echo "  mysqldumppg — mysqldump-compatible dump tool for PostgreSQL"

    # Verify
    if command -v mysqlpg &>/dev/null; then
        echo ""
        echo "Version: $(mysqlpg -V)"
    fi
}

# Uninstall
uninstall() {
    echo "Uninstalling mysqlpg..."
    remove_symlinks
    PYTHON="$(command -v python3 || command -v python)"
    "$PYTHON" -m pip uninstall mysqlpg -y 2>/dev/null || true
    echo "Done."
}

# Main
case "${1:-}" in
    --no-alias|--no-symlink)
        install
        ;;
    --uninstall)
        uninstall
        ;;
    --symlink-only|--alias-only)
        create_symlinks
        ;;
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "  (default)         Install mysqlpg and create mysql/mysqldump symlinks"
        echo "  --no-alias        Install without creating symlinks"
        echo "  --symlink-only    Only create symlinks (skip pip install)"
        echo "  --uninstall       Remove package and symlinks"
        echo "  --help            Show this help"
        echo ""
        echo "Symlinks are created in /usr/local/bin (root) or ~/.local/bin (user)."
        echo "Existing mysql/mysqldump binaries are never overwritten."
        ;;
    *)
        install
        echo ""
        read -p "Create 'mysql' and 'mysqldump' symlinks? [Y/n] " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            create_symlinks
        else
            echo "Skipping symlinks. Create them later with: $0 --symlink-only"
        fi
        ;;
esac
