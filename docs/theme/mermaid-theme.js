// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Mermaid diagrams and the committed SVG figures appear on the same pages, so they
// are given one palette rather than two. Mermaid's stock `default`/`dark` themes are
// not that palette: `dark` fills nodes with neutral #1f2020 against the figures'
// blue-tinted ground, and paints edge labels #cccccc, which is light on a dark page.
//
// The colours therefore come from `theme/diagram-theme.css`, the same `--fig-*`
// tokens the figures read. They are sampled from a probe element carrying the
// figures' own class, so the stylesheet stays the single definition of the palette
// and this file holds only the mapping onto Mermaid's variable names.

(() => {
    const darkThemes = ['ayu', 'navy', 'coal'];
    const lightThemes = ['light', 'rust'];
    const sources = new WeakMap();

    function isDark() {
        return [...document.documentElement.classList].some((name) => darkThemes.includes(name));
    }

    // Read the figure palette exactly as a figure resolves it: the tokens are declared
    // on `.tc-figure` (qualified by the mdBook theme class) rather than on :root, and a
    // directly matching rule beats an inherited value, so the probe must carry the class.
    function palette() {
        const probe = document.createElement('span');
        probe.className = 'tc-figure';
        probe.style.cssText = 'position:absolute;visibility:hidden;width:0;height:0';
        document.body.appendChild(probe);
        const computed = getComputedStyle(probe);
        const read = (name, fallback) =>
            computed.getPropertyValue(name).trim() || fallback;
        const tokens = {
            bg: read('--fig-bg', '#ffffff'),
            edge: read('--fig-edge', '#d0d7de'),
            ink: read('--fig-ink', '#17212b'),
            ink2: read('--fig-ink-2', '#52606d'),
            line: read('--fig-line', '#65758b'),
            panel: read('--fig-panel', '#f7f9fb'),
            panelLine: read('--fig-panel-line', '#a8b3bf'),
            blue: read('--fig-blue', '#e8f1fb'),
            blueLine: read('--fig-blue-line', '#2563a6'),
            green: read('--fig-green', '#e6f5f0'),
            greenLine: read('--fig-green-line', '#147d64'),
            orange: read('--fig-orange', '#fff1e6'),
            orangeLine: read('--fig-orange-line', '#b54708'),
            purple: read('--fig-purple', '#f3ebf8'),
            purpleLine: read('--fig-purple-line', '#8055a3'),
        };
        probe.remove();
        return tokens;
    }

    function themeVariables() {
        const fig = palette();
        return {
            darkMode: isDark(),
            background: fig.bg,
            fontFamily: 'system-ui, sans-serif',
            fontSize: '15px',

            // Flowchart and graph nodes take the figures' panel surface, so a Mermaid
            // node and a drawn node on the same page sit on the same ground.
            primaryColor: fig.panel,
            primaryBorderColor: fig.panelLine,
            primaryTextColor: fig.ink,
            secondaryColor: fig.blue,
            secondaryBorderColor: fig.blueLine,
            secondaryTextColor: fig.ink,
            tertiaryColor: fig.green,
            tertiaryBorderColor: fig.greenLine,
            tertiaryTextColor: fig.ink,
            mainBkg: fig.panel,
            nodeBorder: fig.panelLine,
            nodeTextColor: fig.ink,
            titleColor: fig.ink,
            textColor: fig.ink,
            lineColor: fig.line,

            // A subgraph is a container, not a node, so it takes the page ground and a
            // quiet border rather than another filled surface.
            clusterBkg: fig.bg,
            clusterBorder: fig.edge,

            // The stock value here is an opaque light chip. Matching the page ground lets
            // an edge label sit on its edge in either theme.
            edgeLabelBackground: fig.bg,

            // Sequence diagrams: participants read as panels, notes as the annotation
            // colour the figures already use for staged or deferred work.
            actorBkg: fig.panel,
            actorBorder: fig.panelLine,
            actorTextColor: fig.ink,
            actorLineColor: fig.line,
            signalColor: fig.ink2,
            signalTextColor: fig.ink,
            labelBoxBkgColor: fig.panel,
            labelBoxBorderColor: fig.panelLine,
            labelTextColor: fig.ink,
            loopTextColor: fig.ink,
            noteBkgColor: fig.orange,
            noteBorderColor: fig.orangeLine,
            noteTextColor: fig.ink,
            activationBkgColor: fig.blue,
            activationBorderColor: fig.blueLine,
            altBackground: fig.panel,
            sequenceNumberColor: fig.bg,
        };
    }

    function diagrams() {
        return [...document.querySelectorAll('pre.mermaid')];
    }

    async function render() {
        const nodes = diagrams();
        if (nodes.length === 0) {
            return;
        }
        for (const node of nodes) {
            // Mermaid replaces the source with its output and marks the element done, so
            // re-theming needs the original text back before it will run again.
            if (!sources.has(node)) {
                sources.set(node, node.textContent);
            } else {
                node.removeAttribute('data-processed');
                node.innerHTML = '';
                node.textContent = sources.get(node);
            }
        }
        mermaid.initialize({
            startOnLoad: false,
            theme: 'base',
            themeVariables: themeVariables(),
        });
        await mermaid.run({ nodes, suppressErrors: false });
        // The zoom and fullscreen layer caches each diagram's original viewBox; a
        // re-render produces a fresh <svg>, so that cache has to be dropped.
        document.dispatchEvent(new CustomEvent('dsrv:diagrams-rendered'));
    }

    document.addEventListener('DOMContentLoaded', () => {
        render();
        // Switching theme used to reload the page. Re-rendering in place keeps the
        // reader's scroll position, and any diagram they had zoomed stays where it was.
        for (const name of [...darkThemes, ...lightThemes]) {
            document.getElementById(name)?.addEventListener('click', () => {
                // The click handler that sets the class on <html> may not have run yet.
                window.setTimeout(render, 0);
            });
        }
    });
})();
