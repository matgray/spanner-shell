Spanner Shell CLI

  A curses-based interactive SQL shell for Spanner databases, providing a rich terminal UI for querying your data. This tool enhances the
  command-line experience by wrapping the gcloud spanner execute-sql command with advanced features like proto formatting, ASCII tables, and
  robust scrolling.

  Features

   * Interactive Terminal UI: A full-screen, curses-based application for a comfortable querying experience.
   * Auto Proto Formatting: Proto columns are automatically cast to STRING and then formatted into human-readable, indented text, making
     complex data structures clear.
   * Beautiful ASCII Art Tables: Query results are rendered in classic, well-aligned ASCII art tables with dynamic column sizing.
   * Scrollable Output:
       * Vertical: Navigate large result sets with PgUp / PgDn.
       * Horizontal: Scroll through wide tables using < and > keys.
   * Command History: Access previous queries with Up/Down arrow keys.
   * Enhanced Input:
       * Precise cursor control with Left/Right arrows.
       * Word jumping with Ctrl + Left/Right arrows.
       * Standard Backspace and Delete functionality.
   * Reliable Backend: Leverages the gcloud spanner execute-sql command for robust query execution against Spanner instances (including the
     emulator).
   * Clean UI: Suppresses noisy SDK logs and provides a clean, focused interface.
   * Project Theming: Features custom :ellumDro branding.

  Installationp

 1. Clone the repository:
   1     git clone <your-repo-url>
   2     cd <your-repo-directory>
   2. Ensure Python 3: Make sure you have Python 3 installed.
   3. Dependencies: The script attempts to manage a Python virtual environment (venv) automatically on first run. It will install dependencies
      from requirements.txt.
   4. gcloud CLI: Ensure the Google Cloud SDK (gcloud) is installed and configured, and that you can authenticate with your Spanner instance
      (even the emulator).

  Usage

   1. Make the script executable:
   1     chmod +x scripts/local-dev/spanner_shell.py
   2. Run the Spanner Shell:
   1     ./scripts/local-dev/spanner_shell.py
   3. Connect to Spanner Emulator: Ensure your Spanner emulator is running and accessible, or that your gcloud configuration points to the
      correct instance/project. The script defaults to localhost:9010 for the emulator.
   4. Enter SQL Queries: Type your SQL commands at the prompt (e.g., SELECT * FROM Customer;).
   5. Execute: Press Enter to run the query.

  Key Bindings


   * Enter: Execute query
   * Up/Down Arrow: Navigate command history
   * Left/Right Arrow: Move cursor in input line
   * Ctrl + Left/Right Arrow: Jump cursor by word
   * Backspace/Delete: Edit input line
   * PgUp/PgDn: Scroll results vertically
   * < / >: Scroll results horizontally
   * Q (when input is empty): Quit the shell

  Configuration

  The script uses the following environment variables for configuration:
   * SPANNER_EMULATOR_HOST: Set to localhost:9010 by default.
   * SPANNER_INSTANCE: REQUIRED
   * SPANNER_DATABASE: REQUIRED
   * GOOGLE_CLOUD_PROJECT: REQUIRED

  You can override these by setting them in your environment before running the script.
