# TurtleBot tutorial figure sources

`turtlebot-occlusion.png` reproduces Figure 12 from James Baxter, Bert van
Acker, Morten Kristensen, Thomas Wright, Ana Cavalcanti and Cláudio Gomes,
“Formal Architectural Patterns for Adaptive Robotic Software”, FASE 2025.
Source: <https://doi.org/10.1007/978-3-031-90900-9_8>. © the authors, 2025,
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

`robosapiens-architecture.png` reproduces Figure 2 from Peter G. Larsen et
al., “Robotic safe adaptation in unprecedented situations: the RoboSAPIENS
project”, Research Directions: Cyber-Physical Systems 2, e4, 2024.
Source: <https://doi.org/10.1017/cbp.2024.4>. © the authors, 2024,
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Both images were rendered from the respective paper PDF and cropped to remove
surrounding page text. The artwork is unchanged. Attribution and links to the
licences also appear beside the images in the tutorial.

The four SVG timelines are generated from the checked-in synthetic journals
and clock configuration. From the repository root:

```sh
python3 examples/turtlebot-maplek/generate_figures.py
python3 examples/turtlebot-maplek/generate_figures.py --check
```

They depict recorded evidence and selected property evaluations. They do not
represent measured robot performance or confirm physical recovery.
