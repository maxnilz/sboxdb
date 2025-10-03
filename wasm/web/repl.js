import init, { execute_sql } from './dist/wasm.js';

class WebTerminal {
    constructor() {
        this.consoleDiv = document.getElementById('console');
        this.cmdline = document.getElementById('cmdline');
        this.currentInput = '';
        this.history = [];
        this.historyIndex = -1;
        this.isMultiline = false;

        this.setupEventListeners();
        this.loadHistory();
    }

    setupEventListeners() {
        this.cmdline.addEventListener('keydown', this.handleKeyDown.bind(this));
        this.cmdline.addEventListener('keyup', this.handleKeyUp.bind(this));

        // Focus input when clicking anywhere in terminal
        document.getElementById('terminal').addEventListener('click', () => {
            this.cmdline.focus();
        });

        // Prevent losing focus
        this.cmdline.addEventListener('blur', () => {
            setTimeout(() => this.cmdline.focus(), 10);
        });
    }

    async handleKeyDown(e) {
        switch (e.key) {
            case 'Enter':
                e.preventDefault();
                await this.handleEnter();
                break;
            case 'ArrowUp':
                e.preventDefault();
                this.navigateHistory(-1);
                break;
            case 'ArrowDown':
                e.preventDefault();
                this.navigateHistory(1);
                break;
            case 'Tab':
                e.preventDefault();
                // Could implement tab completion here
                break;
            case 'l':
                if (e.ctrlKey) {
                    e.preventDefault();
                    this.clearScreen();
                }
                break;
            case 'c':
                if (e.ctrlKey) {
                    e.preventDefault();
                    this.cancelInput();
                }
                break;
        }
    }


    handleKeyUp(e) {
        // Update current input for history navigation, but not during multiline input
        if (!['ArrowUp', 'ArrowDown'].includes(e.key) && !this.isMultiline) {
            this.currentInput = this.cmdline.value;
        }
    }

    async handleEnter() {
        const input = this.cmdline.value;

        // Show the input line
        this.print(input, true);
        this.cmdline.value = '';

        // Add to current multiline input or start new input
        if (this.isMultiline) {
            this.currentInput += '\n' + input;
        } else {
            this.currentInput = input;
        }

        // Check if we should continue multiline input
        if (this.shouldContinueMultiline(this.currentInput)) {
            this.isMultiline = true;
            this.updatePrompt('    ..> ');
            return;
        }

        // Reset multiline state
        this.isMultiline = false;
        this.updatePrompt('sboxdb> ');

        // Execute the complete input
        if (this.currentInput.trim()) {
            this.addToHistory(this.currentInput);
            await this.execute(this.currentInput);
        }

        this.currentInput = '';
    }

    shouldContinueMultiline(input) {
        const trimmed = input.trim();

        // Don't continue for commands
        if (trimmed.startsWith('\\')) {
            return false;
        }

        // Continue if doesn't end with semicolon
        return !trimmed.endsWith(';');
    }

    updatePrompt(newPrompt) {
        document.getElementById('prompt').textContent = newPrompt;
    }

    navigateHistory(direction) {
        if (this.history.length === 0) return;

        if (direction === -1) { // Up
            if (this.historyIndex === -1) {
                this.historyIndex = this.history.length - 1;
            } else if (this.historyIndex > 0) {
                this.historyIndex--;
            }
        } else { // Down
            if (this.historyIndex === -1) return;
            if (this.historyIndex < this.history.length - 1) {
                this.historyIndex++;
            } else {
                this.historyIndex = -1;
                this.cmdline.value = this.currentInput;
                return;
            }
        }

        this.cmdline.value = this.history[this.historyIndex];
    }

    addToHistory(input) {
        if (input.trim() && input !== this.history[this.history.length - 1]) {
            this.history.push(input);
            this.saveHistory();
        }
        this.historyIndex = -1;
    }

    loadHistory() {
        try {
            const saved = localStorage.getItem('sboxdb_history');
            if (saved) {
                this.history = JSON.parse(saved);
            }
        } catch (e) {
            console.warn('Failed to load history:', e);
        }
    }

    saveHistory() {
        try {
            // Keep only last 100 entries
            const toSave = this.history.slice(-100);
            localStorage.setItem('sboxdb_history', JSON.stringify(toSave));
        } catch (e) {
            console.warn('Failed to save history:', e);
        }
    }

    clearScreen() {
        this.consoleDiv.innerHTML = '';
        this.print('Welcome to sboxdb shell. Type \\help to learn more.');
    }

    cancelInput() {
        this.cmdline.value = '';
        this.currentInput = '';
        this.isMultiline = false;
        this.updatePrompt('sboxdb> ');
    }

    print(msg, isPrompt = false, isError = false) {
        const div = document.createElement('div');
        div.className = 'line ' + (isPrompt ? 'prompt-line' : isError ? 'error-line' : 'output-line');

        if (isPrompt) {
            div.textContent = (this.isMultiline ? '    ..> ' : 'sboxdb> ') + msg;
        } else {
            div.textContent = msg;
        }

        this.consoleDiv.appendChild(div);
        this.scrollToBottom();
    }

    scrollToBottom() {
        const terminal = document.getElementById('terminal');
        terminal.scrollTop = terminal.scrollHeight;
    }

    helpMsg() {
        return `Enter a SQL statement terminated by a semicolon (;) to execute it and display the result.
The following commands are also available:

    \\help        This help message
    \\q           Quit the shell (reloads page)
    \\d           List tables
    \\d NAME      Describe table
    \\tpcc        Load tpcc dataset

Terminal shortcuts:
    Ctrl+L       Clear screen
    Ctrl+C       Cancel current input
    Up/Down      Navigate command history`;
    }

    async execute(input) {
        if (input.trim() === '') {
            return;
        }

        if (input.startsWith('\\')) {
            await this.executeCommand(input);
        } else {
            await this.executeQuery(input);
        }
    }

    async executeCommand(cmd) {
        const parts = cmd.trim().split(/\s+/);
        console.log(parts);
        try {
            switch (parts[0]) {
                case '\\help':
                    this.print(this.helpMsg());
                    break;
                case '\\q':
                    this.print('Bye!');
                    setTimeout(() => location.reload(), 500);
                    break;
                case '\\d':
                    if (parts[1]) {
                        console.log(parts[1]);
                        await this.executeQuery(`SHOW CREATE TABLE ${parts[1]}`);
                    } else {
                        await this.executeQuery('SHOW TABLES');
                    }
                    break;
                case '\\tpcc':
                    await this.executeQuery('CREATE DATASET tpcc');
                    break;
                default:
                    throw new Error(`Unknown command: ${parts[0]}`);
            }
        } catch (err) {
            this.print('Error: ' + this.getErrorMessage(err), false, true);
        }
    }

    async executeQuery(query) {
        try {
            console.log(query);
            const resultSet = execute_sql(query);
            const result = resultSet.get_result_str();
            if (!result || result.trim() === "") {
                return this.print('done');
            }
            this.print(result);
        } catch (err) {
            this.print('Error: ' + this.getErrorMessage(err), false, true);
        }
    }

    getErrorMessage(err) {
        if (err.get_err_msg) {
            return err.get_err_msg();
        }
        if (err.message) {
            return err.message;
        }
        return String(err);
    }
}

// Initialize the terminal
let terminal;

init().then(() => {
    terminal = new WebTerminal();
    terminal.print('Welcome to sboxdb shell. Type \\help to learn more.');
}).catch(err => {
    console.error('Failed to initialize WASM:', err);
    document.getElementById('console').textContent = 'Failed to load WASM module: ' + err.message;
});