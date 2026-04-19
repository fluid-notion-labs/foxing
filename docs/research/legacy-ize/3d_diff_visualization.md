# 3D Diff Visualization

## Vision

Render version control diffs as textures and display them in a 3D viewer - specifically, showing file diff history as textures mapped onto a sphere. This represents a novel approach to UI for understanding code evolution.

## Current Status

This is a **future research direction** - approximately 4-5 days of focused development away from the current pijul integration work. The pijul integration needs to reach a more complete state before this becomes viable.

## Core Concept

1. **Diff → Texture Pipeline**: Convert diff output (additions, deletions, modifications) into visual textures
   - Color coding for change types (green/red/yellow typical, but could be more nuanced)
   - Line-level or character-level granularity
   - Potentially encode metadata (author, time, change magnitude) into texture properties

2. **3D Sphere Mapping**: Map diff textures onto a spherical surface
   - Timeline as one axis (longitude?)
   - File/directory structure as another axis (latitude?)
   - Or: each commit as a point on the sphere surface with diff texture

3. **Interactive Navigation**: 3D viewer for exploring the diff history
   - Rotate/zoom to explore different time periods
   - Click on regions to drill into specific changes
   - Animations showing evolution over time

## Why This Matters

- **Novel UI paradigm**: Current diff viewers are 2D, linear, and text-focused
- **Pattern recognition**: Humans are good at visual pattern recognition - 3D visualization might reveal patterns invisible in traditional views
- **Engagement**: Makes version control history more approachable and interesting
- **Head-turning potential**: This is genuinely different from existing tools

## Technical Considerations

### Rendering Stack Options
- **wgpu**: Modern, cross-platform GPU abstraction in Rust
- **bevy**: Full game engine, might be overkill but has good 3D primitives
- **three.js/WebGL**: If targeting browser-based viewer
- **vulkan/metal directly**: Maximum control, maximum complexity

### Diff Processing
- Already have pijul integration in progress
- Need to extract structured diff data (not just text)
- Consider libpijul's internal diff representation

### Texture Generation
- Could use image crate for CPU-side texture generation
- Or generate directly on GPU with compute shaders
- Balance between pre-computation and real-time generation

### Sphere Mapping Challenges
- UV mapping for readable text on curved surface
- Level-of-detail for zooming
- Handling large repositories with many files/commits

## Open Questions

1. What's the right mapping from diff data to sphere position?
2. How to handle very large histories without overwhelming the visualization?
3. Should this be standalone app, integrated into ize, or web-based?
4. What interaction patterns make sense for 3D diff exploration?
5. How to maintain readability of actual diff content on curved surface?

## Related Work / Inspiration

- Gource (animated version control visualization)
- Code City (3D software visualization)
- Various git visualization tools (gitk, tig, etc.) - all 2D
- VR code exploration tools (mostly research prototypes)

## Dependencies on Current Work

- Solid pijul integration for extracting diff data
- Potentially: ize's file tracking to understand what changed when
- Clean abstraction layer so visualization doesn't depend on specific VCS

## Timeline Estimate

After pijul integration is "internet-ready" (4-5 days):
- Basic diff-to-texture pipeline: 1-2 days
- Simple 3D viewer with sphere: 1-2 days
- Interactive navigation: 2-3 days
- Polish and refinement: 2-3 days

**Total: ~1-2 weeks of focused development after prerequisites**

---

*This is a "turn heads" feature - prioritize after core functionality is solid.*