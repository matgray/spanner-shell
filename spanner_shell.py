#!/usr/bin/env python3
import os
import sys
import subprocess
import collections
import curses
import json
import re

# --- VIRTUAL ENV SETUP ---
def ensure_venv():
    venv_path = os.path.join(os.getcwd(), "venv")
    venv_python = os.path.join(venv_path, "bin", "python")
    if sys.executable != venv_python:
        if not os.path.isdir(venv_path):
            print("Creating virtual environment...")
            subprocess.run([sys.executable, "-m", "venv", "venv"])
            print("Installing dependencies...")
            subprocess.run([venv_python, "-m", "pip", "install", "-r", "requirements.txt"])
        if os.path.exists(venv_python):
            os.execl(venv_python, venv_python, *sys.argv)
        else:
            sys.exit(1)

ensure_venv()

# Spanner Defaults
SPANNER_EMULATOR_HOST = os.getenv("SPANNER_EMULATOR_HOST", "localhost:9010")
SPANNER_INSTANCE = os.getenv("SPANNER_INSTANCE", "")
SPANNER_DATABASE = os.getenv("SPANNER_DATABASE", "")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")
HISTORY_FILE = "~/.spanner-commands"

from google.cloud import spanner

class SchemaManager:
    def __init__(self, database, log_func=None):
        self.database = database
        self.log_func = log_func
        self.tables = {} # table_name -> [column_names]
        self.refresh()

    def refresh(self):
        try:
            query = "SELECT table_name, column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(query)
                new_tables = collections.defaultdict(list)
                count = 0
                for row in results:
                    new_tables[row[0]].append(row[1])
                    count += 1
                self.tables = dict(new_tables)
                if self.log_func:
                    self.log_func(f"Status: Metadata loaded for {len(self.tables)} tables.")
        except Exception as e:
            if self.log_func:
                self.log_func(f"Status: Schema refresh failed: {e}")

    def rewrite_query(self, query):
        original_query = query
        try:
            match = re.search(r"SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_]+)", query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                actual_table = next((t for t in self.tables if t.lower() == table_name.lower()), None)
                if actual_table:
                    cols = [f"CAST({c} AS STRING) AS {c}" for c in self.tables[actual_table]]
                    col_list = ", ".join(cols)
                    query = re.sub(r"\*", col_list, query, count=1, flags=re.IGNORECASE)
            return query
        except Exception:
            return original_query

class TableRenderer:
    @staticmethod
    def pretty_format_proto(text):
        """State-machine parser to format text protos with indentation and newlines."""
        if not text or (":" not in text and "{" not in text):
            return text

        result = []
        current_line = ""
        in_quotes = False
        indent = 0
        i = 0
        while i < len(text):
            c = text[i]
            if c == '"':
                in_quotes = not in_quotes
                current_line += c
            elif in_quotes:
                current_line += c
            elif c == '{':
                current_line += " {"
                result.append("  " * indent + current_line.strip())
                current_line = ""
                indent += 1
            elif c == '}':
                if current_line.strip():
                    result.append("  " * indent + current_line.strip())
                indent = max(0, indent - 1)
                result.append("  " * indent + "}")
                current_line = ""
            elif c == ' ' and not in_quotes:
                rest = text[i+1:].lstrip()
                if re.match(r'^[a-zA-Z0-9_]+[:{]', rest):
                    if current_line.strip():
                        result.append("  " * indent + current_line.strip())
                    current_line = ""
                else:
                    current_line += c
            else:
                current_line += c
            i += 1
        if current_line.strip():
            result.append("  " * indent + current_line.strip())
        return "\n".join(result) if len(result) > 1 else text

    @staticmethod
    def render(json_data):
        fields = json_data.get("metadata", {}).get("rowType", {}).get("fields", [])
        rows_data = json_data.get("rows", [])
        if not fields: return "No data."

        col_names = [f["name"] for f in fields]
        formatted_rows = []
        for raw_row in rows_data:
            f_row = []
            for val in raw_row:
                val_str = str(val) if val is not None else "NULL"
                if ":" in val_str or "{" in val_str:
                    f_row.append(TableRenderer.pretty_format_proto(val_str))
                else:
                    f_row.append(val_str)
            formatted_rows.append(f_row)

        if not formatted_rows: return "Empty set."

        widths = [len(n) for n in col_names]
        for r in formatted_rows:
            for i, cell in enumerate(r):
                lines = cell.split('\n')
                max_w = max(len(l) for l in lines) if lines else 0
                widths[i] = max(widths[i], max_w)

        def get_sep(edge, fill):
            return edge + edge.join(fill * (w + 2) for w in widths) + edge

        output = []
        sep = get_sep("+", "-")
        output.append(sep)
        output.append("| " + " | ".join(col_names[i].ljust(widths[i]) for i in range(len(col_names))) + " |")
        output.append(sep)

        for r in formatted_rows:
            cell_lines_list = [cell.split('\n') for cell in r]
            row_h = max(len(lines) for lines in cell_lines_list)
            for line_idx in range(row_h):
                line_parts = []
                for col_idx in range(len(col_names)):
                    lines = cell_lines_list[col_idx]
                    content = lines[line_idx] if line_idx < len(lines) else ""
                    line_parts.append(content.ljust(widths[col_idx]))
                output.append("| " + " | ".join(line_parts) + " |")
            output.append(sep)

        output.append(f"({len(rows_data)} row(s) in set)")
        return "\n".join(output)

class SpannerShell:
    def __init__(self):
        os.environ["SPANNER_EMULATOR_HOST"] = SPANNER_EMULATOR_HOST
        self.client = spanner.Client(project=GOOGLE_CLOUD_PROJECT)
        self.instance = self.client.instance(SPANNER_INSTANCE)
        self.db_obj = self.instance.database(SPANNER_DATABASE)

        self.history = []
        self.history_index = -1
        self.current_input = ""
        self.cursor_pos = 0
        self.output_buffer = collections.deque(maxlen=5000)
        self.stdscr = None
        self.scroll_y = 0
        self.scroll_x = 0
        self.h, self.w = 24, 80

        self.schema = SchemaManager(self.db_obj, self.log)
        self.load_history()

    def load_history(self):
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r") as f:
                self.history = [line.strip() for line in f.readlines() if line.strip()]

    def save_history(self, command):
        if not command: return
        if command in self.history: self.history.remove(command)
        self.history.append(command)
        self.history = self.history[-100:]
        with open(HISTORY_FILE, "w") as f:
            for cmd in self.history: f.write(f"{cmd}\n")

    def log(self, text):
        if isinstance(text, str):
            for line in text.split('\n'):
                self.output_buffer.append(line)
        else:
            self.output_buffer.append(str(text))
        self.scroll_y = 0

    def execute_query(self):
        cmd = self.current_input.strip()
        if not cmd: return
        query = self.current_input
        self.save_history(cmd)

        rewritten_query = self.schema.rewrite_query(query)
        self.log(f"{SPANNER_EMULATOR_HOST}> {cmd}")

        self.current_input = ""
        self.cursor_pos = 0
        self.history_index = -1

        if cmd.lower() in ["exit", "quit", "q"]: sys.exit(0)

        self.log("Status: Executing...")
        self.draw()

        try:
            env = os.environ.copy()
            env["SPANNER_EMULATOR_HOST"] = SPANNER_EMULATOR_HOST
            env["PAGER"] = "cat"

            result = subprocess.run([
                "gcloud", "spanner", "databases", "execute-sql",
                SPANNER_DATABASE,
                f"--instance={SPANNER_INSTANCE}",
                f"--project={GOOGLE_CLOUD_PROJECT}",
                "--format=json", "--quiet",
                f"--sql={rewritten_query}"
            ], capture_output=True, text=True, env=env, timeout=60)

            if result.returncode != 0:
                self.log(f"Error (Code {result.returncode}):")
                if result.stderr: self.log(result.stderr)
            else:
                stdout = result.stdout.strip()
                if not stdout:
                    self.log("Success (No output).")
                else:
                    try:
                        data = json.loads(stdout)
                        self.log(TableRenderer.render(data))
                    except Exception as e:
                        self.log(f"Formatting Error: {e}")
                        self.log(stdout)

        except Exception as e:
            self.log(f"Execution Error: {e}")

        self.log("Status: Ready.")
        self.draw()

    def draw(self):
        if not self.stdscr: return
        self.stdscr.erase()
        self.h, self.w = self.stdscr.getmaxyx()

        out_h = self.h - 2
        all_lines = list(self.output_buffer)

        start = max(0, len(all_lines) - out_h - self.scroll_y)
        end = len(all_lines) - self.scroll_y
        display_lines = all_lines[start : (end if self.scroll_y > 0 else None)]

        for i, line in enumerate(display_lines):
            try:
                visible_text = line[self.scroll_x : self.scroll_x + self.w - 1]
                self.stdscr.addstr(i, 0, visible_text)
            except curses.error: pass

        prompt = f"{SPANNER_EMULATOR_HOST}> "
        try:
            input_visible = self.current_input[:self.w-len(prompt)-1]
            self.stdscr.addstr(self.h-1, 0, prompt)
            self.stdscr.addstr(self.h-1, len(prompt), input_visible)
            curses.curs_set(1)
            self.stdscr.move(self.h-1, min(self.w-1, len(prompt) + self.cursor_pos))
        except curses.error: pass
        self.stdscr.refresh()

    def run(self, stdscr):
        self.stdscr = stdscr
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
        stdscr.keypad(True)

        self.log(f"Spanner SQL Shell")
        self.log(f"Connected to {SPANNER_EMULATOR_HOST}")
        self.log("PgUp/PgDn: Vertical Scroll | < / >: Horizontal Scroll")

        while True:
            self.draw()
            try:
                key = stdscr.getch()
            except KeyboardInterrupt: break

            if key in [10, 13]: self.execute_query()
            elif key == curses.KEY_LEFT: self.cursor_pos = max(0, self.cursor_pos - 1)
            elif key == curses.KEY_RIGHT: self.cursor_pos = min(len(self.current_input), self.cursor_pos + 1)
            elif key == ord('<'): self.scroll_x = max(0, self.scroll_x - 20)
            elif key == ord('>'): self.scroll_x = min(10000, self.scroll_x + 20)
            elif key in [545, 560]: # Ctrl+Left
                target = self.current_input.rfind(" ", 0, self.cursor_pos - 1)
                self.cursor_pos = target + 1 if target != -1 else 0
            elif key in [546, 561]: # Ctrl+Right
                target = self.current_input.find(" ", self.cursor_pos + 1)
                self.cursor_pos = target if target != -1 else len(self.current_input)
            elif key == curses.KEY_UP:
                if self.history:
                    if self.history_index == -1: self.history_index = len(self.history) - 1
                    elif self.history_index > 0: self.history_index -= 1
                    self.current_input = self.history[self.history_index]
                    self.cursor_pos = len(self.current_input)
            elif key == curses.KEY_DOWN:
                if self.history_index != -1:
                    if self.history_index < len(self.history) - 1:
                        self.history_index += 1
                        self.current_input = self.history[self.history_index]
                    else:
                        self.history_index = -1; self.current_input = ""
                    self.cursor_pos = len(self.current_input)
            elif key in [curses.KEY_BACKSPACE, 127, 8]:
                if self.cursor_pos > 0:
                    self.current_input = self.current_input[:self.cursor_pos-1] + self.current_input[self.cursor_pos:]
                    self.cursor_pos -= 1
            elif key == ord('q') or key == ord('Q'):
                if not self.current_input: break
                else:
                    self.current_input = self.current_input[:self.cursor_pos] + chr(key) + self.current_input[self.cursor_pos:]
                    self.cursor_pos += 1
            elif 32 <= key <= 126:
                self.current_input = self.current_input[:self.cursor_pos] + chr(key) + self.current_input[self.cursor_pos:]
                self.cursor_pos += 1
            elif key == curses.KEY_PPAGE: self.scroll_y = min(max(0, len(self.output_buffer) - (self.h - 2)), self.scroll_y + (self.h - 5))
            elif key == curses.KEY_NPAGE: self.scroll_y = max(0, self.scroll_y - (self.h - 5))

if __name__ == "__main__":
    try: curses.wrapper(SpannerShell().run)
    except Exception as e: print(f"Shell Error: {e}")
