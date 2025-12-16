# mdBook Quick Reference

## Install mdBook

```bash
cargo install mdbook
```

## Common Commands

### Serve with Live Reload
```bash
cd docs
mdbook serve --open
```
Opens browser at `http://localhost:3000` with auto-reload on file changes.

### Build Documentation
```bash
cd docs
mdbook build
```
Output: `docs/book/` directory with static HTML.

### Test Documentation
```bash
cd docs
mdbook test
```
Tests code examples and checks for issues.

### Clean Build
```bash
cd docs
mdbook clean
```
Removes the `book/` directory.

## Adding New Pages

1. **Create the markdown file**:
   ```bash
   touch docs/src/tutorials/my-new-tutorial.md
   ```

2. **Edit `docs/src/SUMMARY.md`** and add:
   ```markdown
   - [My New Tutorial](./tutorials/my-new-tutorial.md)
   ```

3. **Write your content** in the new `.md` file

4. **Preview**: Save the file and mdbook will auto-reload

## File Structure

```
docs/
├── book.toml           # Configuration
├── src/
│   ├── SUMMARY.md      # Table of contents (edit this!)
│   ├── *.md            # Your content pages
│   └── */              # Organized in folders
└── book/               # Generated output (don't edit)
```

## Markdown Tips

### Code Blocks
````markdown
```rust
fn main() {
    println!("Hello!");
}
```
````

### Links
```markdown
[Link text](./relative/path.md)
[External](https://example.com)
```

### Images
```markdown
![Alt text](./images/diagram.png)
```

### Notes
```markdown
> **Note**: This is important information
```

## Configuration

Edit `docs/book.toml`:

```toml
[book]
title = "Your Title"
authors = ["Your Name"]
language = "en"

[build]
build-dir = "book"

[output.html]
default-theme = "light"
git-repository-url = "https://github.com/user/repo"
```

## Deployment

### GitHub Pages
```bash
cd docs
mdbook build
# Push docs/book/ to gh-pages branch
```

### CI/CD (GitHub Actions)
Create `.github/workflows/docs.yml`:

```yaml
name: Deploy Docs
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Install mdBook
        run: cargo install mdbook
      - name: Build
        run: cd docs && mdbook build
      - name: Deploy
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./docs/book
```

## Troubleshooting

### Port Already in Use
```bash
mdbook serve --port 3001
```

### Changes Not Showing
- Check file is saved
- Check file is listed in SUMMARY.md
- Hard refresh browser (Ctrl+Shift+R)

### Build Errors
```bash
mdbook build --verbose
```

## Current Documentation

- **Pages**: 10 documentation pages
- **Content**: 3,265 lines of documentation
- **Sections**:
  - Getting Started (Installation, Quick Start)
  - Tutorials (MQTT, ROS2, Distributed)
  - Advanced (Lola Specs, Custom Messages)
  - Reference (CLI, Configuration)

## Resources

- [mdBook Docs](https://rust-lang.github.io/mdBook/)
- [Markdown Guide](https://www.markdownguide.org/)
- Project docs: `docs/README.md`

---

**Ready to start?** Run `mdbook serve --open` in the `docs/` directory!
