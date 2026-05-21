#!/usr/bin/env bash
# Install stepflow in a venv and register CLI commands.
#
# Usage:
#   scripts/install.sh                     # install from current repo
#   curl -sSL https://.../install.sh | bash  # clone + install (future)
#
# Creates a venv at ~/.local/share/stepflow/venv/ and registers these
# commands in ~/.local/bin/:
#   stepflow-lint     — validate pipeline YAML files
#   stepflow-run      — interactive pipeline runner
#   stepflow-convert  — skill description → pipeline YAML

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="${HOME}/.local/share/stepflow/venv"
BIN_DIR="${HOME}/.local/bin"

echo "=== Stepflow Install ==="
echo "Repo:  $REPO_DIR"
echo "Venv:  $VENV_DIR"
echo "Bin:   $BIN_DIR"
echo ""

# 1. Create venv (idempotent)
if [ -d "$VENV_DIR" ]; then
    echo "→ Venv already exists, upgrading..."
else
    echo "→ Creating venv..."
    python3 -m venv "$VENV_DIR"
fi

# 2. Install stepflow into the venv
echo "→ Installing stepflow..."
"$VENV_DIR/bin/pip" install -e "$REPO_DIR" --quiet
echo "  ✓ stepflow installed"

# 3. Create bin directory
mkdir -p "$BIN_DIR"

# 4. Create wrapper scripts pointing at the venv
cat > "$BIN_DIR/stepflow-lint" << SCRIPT
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/python3" "${REPO_DIR}/src/stepflow/plugins/linter/cli.py" "\$@"
SCRIPT
chmod +x "$BIN_DIR/stepflow-lint"
echo "  ✓ stepflow-lint     → $BIN_DIR/stepflow-lint"

cat > "$BIN_DIR/stepflow-run" << SCRIPT
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/python3" "${REPO_DIR}/scripts/skill_repl.py" "\$@"
SCRIPT
chmod +x "$BIN_DIR/stepflow-run"
echo "  ✓ stepflow-run      → $BIN_DIR/stepflow-run"

cat > "$BIN_DIR/stepflow-convert" << SCRIPT
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/python3" "${REPO_DIR}/scripts/skill_convert.py" "\$@"
SCRIPT
chmod +x "$BIN_DIR/stepflow-convert"
echo "  ✓ stepflow-convert  → $BIN_DIR/stepflow-convert"

# 5. Check PATH
if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
    echo ""
    echo "⚠  Add $BIN_DIR to your PATH:"
    echo ""
    echo "    echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
    echo "    source ~/.bashrc"
    echo ""
else
    echo ""
    echo "✓ $BIN_DIR is already in PATH"
fi

echo "=== Done ==="
echo ""
echo "Try: stepflow-lint --help"
echo "     stepflow-run <graph.yaml>"
echo "     stepflow-convert <description.md> -o pipeline.yaml"
