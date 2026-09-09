(() => {
    function makeInlineFigureIdsUnique() {
        const used = new Set();

        document.querySelectorAll('svg.tc-figure').forEach((figure, figureIndex) => {
            const renamed = new Map();

            figure.querySelectorAll('[id]').forEach((element) => {
                const original = element.id;
                let replacement = original;
                let suffix = figureIndex + 1;
                while (used.has(replacement)) {
                    replacement = `${original}-${suffix}`;
                    suffix += 1;
                }
                used.add(replacement);
                if (replacement !== original) {
                    element.id = replacement;
                    renamed.set(original, replacement);
                }
            });

            if (renamed.size === 0) {
                return;
            }
            [figure, ...figure.querySelectorAll('*')].forEach((element) => {
                for (const attribute of Array.from(element.attributes)) {
                    let value = attribute.value;
                    renamed.forEach((replacement, original) => {
                        value = value.replaceAll(
                            `url(#${original})`,
                            `url(#${replacement})`,
                        );
                        if (value === `#${original}`) {
                            value = `#${replacement}`;
                        }
                        if (attribute.name.startsWith('aria-')) {
                            value = value
                                .split(/\s+/)
                                .map((token) => token === original ? replacement : token)
                                .join(' ');
                        }
                    });
                    if (value !== attribute.value) {
                        element.setAttribute(attribute.name, value);
                    }
                }
            });
        });
    }

    document.addEventListener('DOMContentLoaded', makeInlineFigureIdsUnique);
})();
