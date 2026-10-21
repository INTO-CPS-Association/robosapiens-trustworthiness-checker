# Editor support

The DSRV language has editor support for Visual Studio Code, provided by two separate components:

- the **[dsrv-vscode](https://github.com/INTO-CPS-Association/dsrv-vscode)** extension, which supplies syntax highlighting, the `.dsrv` file association, and commands for running a model;
- the **[dsrv-lsp](https://github.com/INTO-CPS-Association/dsrv-lsp)** language server, which supplies completion and diagnostics over the Language Server Protocol.

The language server links the Trustworthiness Checker (TC) as a library and reuses its parser and type checker, so editor diagnostics come from the same language definition the TC evaluates, though not necessarily from the same revision of it. See [When diagnostics and runs disagree](#when-diagnostics-and-runs-disagree).

Both executables ship inside the extension. Installing it is the whole setup; nothing has to be built first.

## What the extension provides

| Feature | Component | Notes |
|---|---|---|
| Syntax highlighting for `.dsrv` files | Extension | Static grammar; no binary required |
| Keyword and function completion | Language server | Not yet context-aware |
| Syntax and type diagnostics | Language server | Reported as editor squiggles |
| Run the current model | Extension | Runs the checker as a VS Code task |

## Install the extension

Search for **DSRV Language Support** in the Extensions view (<kbd>Ctrl</kbd>+<kbd>shift</kbd>+<kbd>X</kbd>) and install it.

The extension is published as one package per platform, and the Marketplace sends you the one for yours. To install it from a file instead, take the `.vsix` for your platform from the [releases](https://github.com/INTO-CPS-Association/dsrv-vscode/releases) and run:

````sh
code --install-extension <vsix-file>
````

## Bundled binaries

The extension contains a `dsrv-lsp` and a `trustworthiness_checker` built for your platform, so completion, diagnostics and running a model all work on a fresh install.

Each release pins the exact version of each executable it ships, rather than tracking the latest; `bundled-binaries.json` in the extension's repository records which ones a given version contains.

## Using your own build

Set either path setting and the extension uses your executable instead of the bundled one.

| Setting | Points at |
| --- | --- |
| `DSRV.binaryPath` | A `trustworthiness_checker` of your own |
| `DSRV.lspPath` | A `dsrv-lsp` of your own |

Each accepts the executable itself, a directory containing it, or a bare command name to resolve from `PATH`. Given a Cargo checkout it also looks in `target/releases` and `target/debug`, so pointing the setting at the repository root is enough.

The extension looks at the setting first, then at the bundled binary, then at `PATH`. An empty setting, the default, means the bundled binary is used.

To build the executables yourself, see [Getting started](../getting-started.md) for the checker, and build in release mode from the repository root:

```sh
cargo build --release --package trustworthiness_checker
```

The executable is written to `target/release/trustworthiness_checker`. Build the language server the same way from a `dsrv-lsp` checkout, which writes `target/release/dsrv-lsp`.

## Run a model from the editor

Open a `.dsrv` file and press the run button in the editor bar.

| Command | What it does |
| --- | --- |
| **DSRV: Run** | Runs the model in the active editor with the configured semantics. Uses `<name>.input` when that file sits beside the model, and otherwise asks you to choose an input file. |
| **DSRV: Run with...** | The same, but asks which semantics to use for this run only, leaving the setting unchanged. |
| **DSRV: Select semantics** | Changes the configured semantics. This is also what the status bar item does when clicked. |

The command invoked is:

```text
<trustworthiness_checker> <model.dsrv> --input-file <input> \
    --language dsrv [--semantics <semantics>] [--runtime <runtime>] \
    --output-stdout
```

The run appears in its own terminal as a VS Code task, which echoes that command line at the top.

## Choosing semantics and a runtime

While a `.dsrv` file is open, the status bar shows which semantics a run will use. Click it to change the setting.

| Setting | Default | Meaning |
| --- | --- | --- |
| `DSRV.semantics` | empty | Which semantics to monitor with. Empty passes no `--semantics`, so the checker's own default applies. |
| `DSRV.runtime` | empty | Which runtime to execute on. Empty passes no `--runtime`, so the checker's own default applies. |

Both are empty by default, which means running from the editor is the same command as running from a shell. The status bar shows `DSRV: checker default` in that case, because the default belongs to the checker and the extension does not restate it.

## Semantics

| Value | Meaning |
| --- | --- |
| `untimed` | Gradual typing: checks the annotations you wrote, infers the ones you did not, and uses type information during evaluation where supported. |
| `typed-untimed` | Strict typing: requries annotations on all stream variables. |
| `causal` | Produces values with causal sets identifying which input observations support them. |

The checker accepts further semantics that target MLTL and STL; the extension does not offer them, because it supports DSRV only. See the [CLI reference](../reference/cli.md) for the full set.

## Runtimes

`causal` only runs on the semi-synchronous runtime, so selecting it makes the extension pass `--runtime semi-sync` whatever `DSRV.runtime` says. The status bar shows this as `DSRV: causal · semi-sync`, and the echoed command line shows the flag.

## Troubleshooting

**No completion or diagnostics.** The language server did not start. Check the `DSRV` output channel (**View -> Output**, then select `DSRV`). A message of the form `Failed to start dsrv-lsp: spawn dsrv-lsp ENOENT` means the executable was not found, which on a default installation should not happen. Check whether `DSRV.lspPath` is set and points somewhere that no longer exists. Clearing it falls back to the bundled server.

Setting `dsrv-lsp.trace.server` to `verbose` logs the traffic between the editor and the server in the same output channel.

**A run reports that the checker could not be found.** The same applies to `DSRV.binaryPath`: clear it to go back to the bundled checker. The error message offers to locate the executable for you, which writes the path into the setting.

**A run exits immediately with a usage error.** Read the echoed command line in the terminal. A `--semantics` or `--runtime` value edited by hand in `settings.json` may not be one your checker accepts; the extension's dropdowns only offer values that work with the bundled one.

<a id="when-diagnostics-and-runs-disagree"></a>
 
**Diagnostics disagree with what a run accepts.** The two bundled executables contain copies of the DSRV front end built from different revisions of the checker, so in edge cases around typing they can disagree: the editor may not flag something a run rejects, or the other way round. When they disagree, the run is the authority.

If you build both yourself, rebuild `dsrv-lsp` against the checker revision you are running and the two agree. With the bundled binaries there is nothing to adjust; closing this gap is the next piece of work on the language server.
