(() => {
    // Two kinds of figure get the same controls: Mermaid, which renders an <svg> inside a
    // <pre class="mermaid">, and a committed figure inlined by {{#include}}, which *is* the
    // <svg>. Both expose a viewBox, so pan and zoom work identically on either.
    const diagramSelector = 'pre.mermaid, svg.tc-figure';
    const frameSelector = '.mermaid-fullscreen-frame';
    const fallbackClass = 'mermaid-fullscreen-fallback';
    const minScale = 0.5;
    const maxScale = 8;
    const views = new WeakMap();

    function fullscreenFrame() {
        return document.fullscreenElement?.matches(frameSelector)
            ? document.fullscreenElement
            : document.querySelector(`${frameSelector}.${fallbackClass}`);
    }

    function isFullscreen(frame) {
        return document.fullscreenElement === frame || frame.classList.contains(fallbackClass);
    }

    function viewFor(frame) {
        let view = views.get(frame);
        if (!view) {
            view = {
                scale: 1,
                original: null,
                box: null,
                pointerId: null,
                pointerX: 0,
                pointerY: 0,
            };
            views.set(frame, view);
        }
        return view;
    }

    function diagramIn(frame) {
        return frame.querySelector(diagramSelector);
    }

    function svgIn(frame) {
        const diagram = diagramIn(frame);
        if (!diagram) {
            return null;
        }
        return diagram.tagName.toLowerCase() === 'svg' ? diagram : diagram.querySelector('svg');
    }

    function ensureSvgView(frame) {
        const svg = svgIn(frame);
        if (!svg) {
            return null;
        }
        const view = viewFor(frame);
        if (!view.original) {
            const values = (svg.getAttribute('viewBox') || '')
                .trim()
                .split(/[ ,]+/)
                .map(Number);
            if (values.length !== 4 || values.some((value) => !Number.isFinite(value))) {
                return null;
            }
            const [x, y, width, height] = values;
            if (width <= 0 || height <= 0) {
                return null;
            }
            view.original = { x, y, width, height };
            view.box = { ...view.original };
        }
        return { svg, view };
    }

    function renderMetrics(viewport, box) {
        const rect = viewport.getBoundingClientRect();
        const pixelsPerUnit = Math.min(rect.width / box.width, rect.height / box.height);
        const renderedWidth = box.width * pixelsPerUnit;
        const renderedHeight = box.height * pixelsPerUnit;
        return {
            rect,
            pixelsPerUnit,
            left: rect.left + (rect.width - renderedWidth) / 2,
            top: rect.top + (rect.height - renderedHeight) / 2,
            renderedWidth,
            renderedHeight,
        };
    }

    function applyView(frame) {
        const current = ensureSvgView(frame);
        const view = viewFor(frame);
        if (current) {
            const { svg } = current;
            const { x, y, width, height } = view.box;
            svg.setAttribute('viewBox', `${x} ${y} ${width} ${height}`);
        }
        const reset = frame.querySelector('.mermaid-reset-button');
        if (reset) {
            reset.textContent = `${Math.round(view.scale * 100)}%`;
            reset.title = `Reset diagram view (${Math.round(view.scale * 100)}%)`;
        }
    }

    function resetView(frame) {
        const current = ensureSvgView(frame);
        const view = viewFor(frame);
        view.scale = 1;
        if (current) {
            view.box = { ...view.original };
        }
        view.pointerId = null;
        frame.classList.remove('mermaid-is-panning');
        applyView(frame);
    }

    function zoomAt(frame, factor, clientX, clientY) {
        if (!isFullscreen(frame)) {
            return;
        }
        const viewport = diagramIn(frame);
        const current = ensureSvgView(frame);
        if (!viewport || !current) {
            return;
        }
        const { view } = current;
        const nextScale = Math.min(maxScale, Math.max(minScale, view.scale * factor));
        if (nextScale === view.scale) {
            return;
        }
        const metrics = renderMetrics(viewport, view.box);
        const pointX = clientX ?? metrics.rect.left + metrics.rect.width / 2;
        const pointY = clientY ?? metrics.rect.top + metrics.rect.height / 2;
        const normalizedX = Math.min(1, Math.max(0, (pointX - metrics.left) / metrics.renderedWidth));
        const normalizedY = Math.min(1, Math.max(0, (pointY - metrics.top) / metrics.renderedHeight));
        const focusX = view.box.x + normalizedX * view.box.width;
        const focusY = view.box.y + normalizedY * view.box.height;
        const width = view.original.width / nextScale;
        const height = view.original.height / nextScale;
        view.box = {
            x: focusX - normalizedX * width,
            y: focusY - normalizedY * height,
            width,
            height,
        };
        view.scale = nextScale;
        applyView(frame);
    }

    function updateButton(frame, expanded) {
        const button = frame.querySelector('.mermaid-fullscreen-toggle');
        if (!button) {
            return;
        }
        button.setAttribute('aria-expanded', String(expanded));
        button.setAttribute('aria-label', expanded ? 'Exit full screen diagram' : 'View diagram full screen');
        button.title = expanded ? 'Exit full screen' : 'View full screen';
        button.querySelector('.mermaid-fullscreen-label').textContent = expanded
            ? 'Exit full screen'
            : 'Full screen';
    }

    function leaveFallback(frame) {
        frame.classList.remove(fallbackClass);
        document.body.classList.remove('mermaid-fullscreen-open');
        updateButton(frame, false);
        resetView(frame);
    }

    async function toggleFullscreen(frame) {
        if (document.fullscreenElement === frame) {
            await document.exitFullscreen();
            return;
        }

        const fallback = document.querySelector(`${frameSelector}.${fallbackClass}`);
        if (fallback === frame) {
            leaveFallback(frame);
            return;
        }
        if (fallback) {
            leaveFallback(fallback);
        }

        if (frame.requestFullscreen) {
            try {
                await frame.requestFullscreen();
                return;
            } catch (_error) {
                // Fall through to the fixed-position presentation below.
            }
        }

        frame.classList.add(fallbackClass);
        document.body.classList.add('mermaid-fullscreen-open');
        updateButton(frame, true);
        resetView(frame);
    }

    function createToolbarButton(className, label, text, handler) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `mermaid-fullscreen-button ${className}`;
        button.setAttribute('aria-label', label);
        button.title = label;
        button.textContent = text;
        button.addEventListener('click', handler);
        return button;
    }

    function installPanAndZoom(frame, diagram) {
        diagram.addEventListener('wheel', (event) => {
            if (!isFullscreen(frame)) {
                return;
            }
            event.preventDefault();
            zoomAt(frame, Math.exp(-event.deltaY * 0.0015), event.clientX, event.clientY);
        }, { passive: false });

        diagram.addEventListener('pointerdown', (event) => {
            if (!isFullscreen(frame) || (event.button !== 0 && event.button !== 1)) {
                return;
            }
            const view = viewFor(frame);
            view.pointerId = event.pointerId;
            view.pointerX = event.clientX;
            view.pointerY = event.clientY;
            diagram.setPointerCapture(event.pointerId);
            frame.classList.add('mermaid-is-panning');
            event.preventDefault();
        });

        diagram.addEventListener('pointermove', (event) => {
            const current = ensureSvgView(frame);
            const view = viewFor(frame);
            if (!current || view.pointerId !== event.pointerId) {
                return;
            }
            const metrics = renderMetrics(diagram, view.box);
            view.box.x -= (event.clientX - view.pointerX) / metrics.pixelsPerUnit;
            view.box.y -= (event.clientY - view.pointerY) / metrics.pixelsPerUnit;
            view.pointerX = event.clientX;
            view.pointerY = event.clientY;
            applyView(frame);
        });

        const stopPanning = (event) => {
            const view = viewFor(frame);
            if (view.pointerId !== event.pointerId) {
                return;
            }
            view.pointerId = null;
            frame.classList.remove('mermaid-is-panning');
        };
        diagram.addEventListener('pointerup', stopPanning);
        diagram.addEventListener('pointercancel', stopPanning);
        diagram.addEventListener('auxclick', (event) => {
            if (isFullscreen(frame) && event.button === 1) {
                event.preventDefault();
            }
        });
        diagram.addEventListener('dblclick', (event) => {
            if (isFullscreen(frame)) {
                event.preventDefault();
                resetView(frame);
            }
        });
    }

    function enhanceDiagram(diagram, index) {
        if (diagram.closest(frameSelector)) {
            return;
        }
        if (!diagram.id) {
            const kind = diagram.tagName.toLowerCase() === 'svg' ? 'figure' : 'mermaid-diagram';
            diagram.id = `${kind}-${index + 1}`;
        }

        const frame = document.createElement('div');
        frame.className = 'mermaid-fullscreen-frame';
        const toolbar = document.createElement('div');
        toolbar.className = 'mermaid-fullscreen-toolbar';
        const help = document.createElement('span');
        help.className = 'mermaid-fullscreen-help';
        help.textContent = 'Mouse wheel to zoom · Drag to pan · Double-click to reset';
        const controls = document.createElement('span');
        controls.className = 'mermaid-zoom-controls';
        controls.appendChild(createToolbarButton(
            'mermaid-zoom-out-button',
            'Zoom out diagram',
            '−',
            () => zoomAt(frame, 1 / 1.25),
        ));
        controls.appendChild(createToolbarButton(
            'mermaid-reset-button',
            'Reset diagram view',
            '100%',
            () => resetView(frame),
        ));
        controls.appendChild(createToolbarButton(
            'mermaid-zoom-in-button',
            'Zoom in diagram',
            '+',
            () => zoomAt(frame, 1.25),
        ));

        const button = createToolbarButton(
            'mermaid-fullscreen-toggle',
            'View diagram full screen',
            '',
            () => toggleFullscreen(frame),
        );
        button.setAttribute('aria-controls', diagram.id);
        button.setAttribute('aria-expanded', 'false');
        button.innerHTML = '<span aria-hidden="true">⛶</span><span class="mermaid-fullscreen-label">Full screen</span>';
        toolbar.appendChild(help);
        toolbar.appendChild(controls);
        toolbar.appendChild(button);

        diagram.parentNode.insertBefore(frame, diagram);
        frame.appendChild(toolbar);
        frame.appendChild(diagram);
        installPanAndZoom(frame, diagram);
    }

    function enhanceAllDiagrams() {
        document.querySelectorAll(diagramSelector).forEach(enhanceDiagram);
    }

    document.addEventListener('DOMContentLoaded', enhanceAllDiagrams);
    // Re-theming a Mermaid diagram replaces its <svg>, so the cached original viewBox
    // that pan and zoom restore to no longer refers to a live element.
    document.addEventListener('dsrv:diagrams-rendered', () => {
        document.querySelectorAll(frameSelector).forEach((frame) => views.delete(frame));
        enhanceAllDiagrams();
    });
    document.addEventListener('fullscreenchange', () => {
        document.querySelectorAll(frameSelector).forEach((frame) => {
            const expanded = document.fullscreenElement === frame;
            updateButton(frame, expanded);
            resetView(frame);
        });
    });
    document.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape' || document.fullscreenElement) {
            return;
        }
        const frame = fullscreenFrame();
        if (frame?.classList.contains(fallbackClass)) {
            leaveFallback(frame);
        }
    });
})();
