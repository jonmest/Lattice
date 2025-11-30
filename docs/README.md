# Documentation

## Generating Architecture Diagram

The architecture diagram can be generated from `architecture.mmd` using:

**Online:**
- https://mermaid.live - paste the content and export as PNG/SVG

**CLI:**
```bash
# Install mermaid-cli
npm install -g @mermaid-js/mermaid-cli

# Generate PNG
mmdc -i architecture.mmd -o architecture.png -b transparent
```

**VS Code:**
- Install the "Markdown Preview Mermaid Support" extension
- Open the README and it'll render automatically

Save the output as `architecture.png` in this directory.
