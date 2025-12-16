# RoboSAPIENS Trustworthiness Checker Documentation

This directory contains the documentation for the RoboSAPIENS Trustworthiness Checker, built using [mdBook](https://rust-lang.github.io/mdBook/).

## Quick Start

### Install mdBook

```bash
cargo install mdbook
```

### Serve Locally

To build and serve the documentation locally with live reload:

```bash
cd docs
mdbook serve --open
```

The documentation will be available at `http://localhost:3000` and will automatically reload when you make changes.

### Build Only

To build the documentation without serving:

```bash
cd docs
mdbook build
```

The built documentation will be in `docs/book/`.

## Documentation Structure

```
docs/
├── book.toml                    # mdBook configuration
├── src/                         # Markdown source files
│   ├── SUMMARY.md              # Table of contents
│   ├── introduction.md         # Introduction page
│   ├── getting-started/        # Getting started guides
│   │   ├── installation.md
│   │   └── quick-start.md
│   ├── tutorials/              # Tutorial pages
│   │   ├── mqtt-integration.md
│   │   ├── ros2-integration.md
│   │   └── distributed-monitoring.md
│   ├── advanced/               # Advanced topics
│   │   ├── lola-specifications.md
│   │   └── custom-messages.md
│   └── reference/              # Reference documentation
│       ├── cli-options.md
│       └── configuration.md
└── book/                       # Generated output (git-ignored)
```

## Writing Documentation

### Adding a New Page

1. Create a new `.md` file in the appropriate directory under `src/`
2. Add an entry to `src/SUMMARY.md` in the correct location
3. The page will automatically appear in the navigation

### Markdown Features

mdBook supports standard Markdown plus some extensions:

- **Code blocks with syntax highlighting**:
  ````markdown
  ```rust
  fn main() {
      println!("Hello, world!");
  }
  ```
  ````

- **Internal links**:
  ```markdown
  [Link text](./relative/path.md)
  ```

- **Admonitions** (with mdbook-admonish plugin):
  ```markdown
  > **Note**: This is a note
  ```

### Best Practices

- Use descriptive headings with proper hierarchy (`#`, `##`, `###`)
- Include practical examples and code snippets
- Link to related pages for cross-referencing
- Keep pages focused on a single topic
- Use consistent terminology throughout

## Customization

### Configuration

Edit `book.toml` to customize:
- Book title and author
- Output directory
- Theme settings
- Additional preprocessors or renderers

### Theme

The default theme can be customized by adding theme files to a `theme/` directory.

## Deployment

### GitHub Pages (Automated)

The documentation is automatically deployed to GitHub Pages via GitHub Actions whenever changes are pushed to the `docs/` directory on the `main` branch.

**Setup Instructions**: See [GITHUB_PAGES_SETUP.md](./GITHUB_PAGES_SETUP.md) for complete setup guide.

**Quick Setup**:
1. Enable GitHub Pages in repository Settings → Pages
2. Set source to "GitHub Actions"
3. Push changes to `docs/` on `main` branch

The workflow (`.github/workflows/deploy-docs.yaml`) automatically:
- Installs mdBook
- Builds the documentation
- Deploys to GitHub Pages

**Manual deployment** can be triggered from the Actions tab.

### Local Build

To build locally without deploying:

```bash
cd docs
mdbook build
```

Output will be in `docs/book/`.

## Contributing

When contributing to the documentation:

1. Follow the existing structure and style
2. Test your changes locally with `mdbook serve`
3. Check for broken links
4. Ensure code examples are correct
5. Update the table of contents in `SUMMARY.md` if adding new pages

## Resources

- [mdBook Documentation](https://rust-lang.github.io/mdBook/)
- [Markdown Guide](https://www.markdownguide.org/)
- [RoboSAPIENS Project](https://robosapiens.eu/)

## Support

The work presented here is supported by the RoboSAPIENS project funded by the European Commission's Horizon Europe programme under grant agreement number 101133807.
