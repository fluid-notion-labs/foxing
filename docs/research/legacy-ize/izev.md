# Izev - TUI Interface for Ize

## Overview

Izev is a Terminal User Interface (TUI) for interacting with Ize version-controlled filesystems. It provides an intuitive, keyboard-driven interface for managing projects, viewing automatically ingested changes, and managing version control operations.

## Core Concepts

### Tabs

Izev uses a two-level tabbed interface:

**Main Tabs:**
| Tab | Description | Status |
|-----|-------------|--------|
| **Projects** | List of all tracked Ize projects | ✅ Implemented |
| **Channels** | View changes in different channels | ✅ Implemented |

**Channel Sub-tabs (within Channels tab):**
| Channel | Description | Status |
|---------|-------------|--------|
| **Stream** | Automatically ingested changes (auto-save) | ✅ Implemented |
| **Main** | Checkpointed/coalesced changes | 🔜 Planned |

### Channel Concept

Ize automatically ingests file changes into a **Stream** channel. This captures every file modification in real-time. The Main channel (future) will contain user-curated "commits" - meaningful checkpoints coalesced from the stream.

## Architecture

### Crate Structure

```
crates/izev/
├── Cargo.toml
└── src/
    ├── main.rs      # Entry point, CLI argument parsing
    ├── app.rs       # Application state, tabs, project/change management
    ├── event.rs     # Event handling (crossterm integration)
    ├── tui.rs       # Terminal management
    └── ui.rs        # UI rendering (tabbed view, tables, overlays)
```

### Dependencies

| Crate | Purpose |
|-------|---------|
| `ize-lib` | Core Ize library integration (ProjectManager, etc.) |
| `thiserror` | Custom error types |
| `anyhow` | Error propagation |
| `clap` | CLI argument parsing |
| `ratatui` | TUI framework |
| `crossterm` | Terminal backend |

### Module Responsibilities

#### `main.rs`
- CLI argument parsing with clap
- Subcommand dispatch (tui, status)
- Main event loop orchestration

#### `app.rs`
- Application state (`App` struct)
- Mode management (Normal, Command, Help)
- Tab tracking (Projects, Stream)
- ProjectManager integration
- Change entries and selection
- Event handling and state transitions
- Command execution

#### `event.rs`
- Crossterm event polling
- Event abstraction layer
- Keyboard input mapping

#### `tui.rs`
- Terminal initialization/cleanup
- Raw mode management
- Alternate screen handling
- Frame rendering dispatch

#### `ui.rs`
- Tabbed layout composition
- Tab bar rendering
- Projects table view
- Stream changes table view
- Status bar with position/tab indicators
- Help overlay
- Widget styling

## UI Layout

### Projects Tab
```
┌─────────────────────────────────────────────────────────────────┐
│ Izev - .                                                        │
│  [ Projects ]  [ Channels ]                                     │  <- Main Tabs
├─────────────────────────────────────────────────────────────────┤
│ Projects (3)                                                    │
├──────────────────────┬──────────┬────────────────────┬──────────┤
│ Source Directory     │ UUID     │ Created            │ Channel  │
├──────────────────────┼──────────┼────────────────────┼──────────┤
│ my-project           │ a1b2c3d. │ 2024-01-15         │ main     │  <- Selected
│ another-app          │ e4f5g6h. │ 2024-01-10         │ main     │
│ test-repo            │ i7j8k9l. │ 2024-01-05         │ main     │
├─────────────────────────────────────────────────────────────────┤
│ Press '?' for help               Projects   1/3       NORMAL    │
└─────────────────────────────────────────────────────────────────┘
```

### Channels Tab (with project selected)
```
┌─────────────────────────────────────────────────────────────────┐
│ Izev - my-project                                               │
│  [ Projects ]  [ Channels ]                                     │  <- Main Tabs
├─────────────────────────────────────────────────────────────────┤
│  [ Stream ]  [ Main ]                                           │  <- Channel Sub-tabs
├─────────────────────────────────────────────────────────────────┤
│ Stream - my-project                                             │
├──────────┬────────────────────────────────┬──────────┬──────────┤
│ ID       │ Summary                        │ Time     │ Files    │
├──────────┼────────────────────────────────┼──────────┼──────────┤
│ a1b2c3d  │ Auto-save: modified src/main.rs│ 2 min ago│ 1        │  <- Selected
│ e4f5g6h  │ Auto-save: modified lib.rs     │ 5 min ago│ 2        │
│ i7j8k9l  │ Auto-save: created utils.rs    │ 12 min   │ 1        │
├─────────────────────────────────────────────────────────────────┤
│ Press '?' for help               Channels  1/3       NORMAL     │
└─────────────────────────────────────────────────────────────────┘
```

## Modes

### Normal Mode
- Default browsing mode
- Navigate items with j/k or arrow keys
- Tab/arrows to switch between tabs
- Enter to select item
- Press `:` to enter command mode
- Press `?` or `h` for help

### Command Mode
- Vim-style command input
- Execute with Enter
- Cancel with Escape
- Commands: `q`, `quit`, `help`, `projects`, `channels`, `refresh`

### Help Mode
- Overlay displaying keyboard shortcuts
- Press any key to dismiss

## Keyboard Shortcuts

### Global
| Key | Action |
|-----|--------|
| `q` | Quit |
| `?` / `h` | Show help |
| `j` / `↓` | Move down in list |
| `k` / `↑` | Move up in list |
| `g` / `Home` | Go to top |
| `G` / `End` | Go to bottom |
| `PgUp` / `PgDn` | Page up/down |
| `Enter` | Select item |
| `:` | Enter command mode |
| `Ctrl+C` | Force quit |

### Navigation
| Key | Action |
|-----|--------|
| `Tab` | Next main tab |
| `Alt+P` | Jump to Projects tab |
| `Alt+C` | Jump to Channels tab |
| `←` / `[` | Previous channel (in Channels tab) |
| `→` / `]` | Next channel (in Channels tab) |

### Projects Tab
| Key | Action |
|-----|--------|
| `r` | Refresh projects list |
| `Enter` | Select project & switch to Channels |

## Workflow

1. **Launch izev** - Opens to Projects tab
2. **View projects** - See all tracked Ize projects from `~/.local/share/ize/projects/`
3. **Select project** - Press Enter to select a project
4. **View channels** - Automatically switches to Channels tab with Stream channel active
5. **Switch channels** - Use `[` / `]` to switch between Stream and Main (future)
6. **Navigate changes** - Browse through auto-recorded changes
7. **View details** - Press Enter on a change to see details (future)
8. **Quick jump** - Press `Alt+P` to jump to Projects or `Alt+C` for Channels

## Future Enhancements

### Phase 1: Integration
- [ ] Connect to ize-lib for real Stream channel data
- [ ] Display actual auto-ingested changes from PijulBackend
- [ ] Show diff content when selecting a change
- [ ] Real-time updates as new changes are ingested
- [ ] Load channels dynamically from project metadata

### Phase 2: Main Channel
- [ ] Implement Main channel sub-tab functionality
- [ ] Checkpoint creation from Stream changes
- [ ] Coalescing multiple stream entries into single commit
- [ ] Commit message editing
- [ ] Channel switching with `[` / `]` keys

### Phase 3: Change Details
- [ ] Detailed change view (files affected, full diff)
- [ ] File content preview
- [ ] Search/filter changes
- [ ] Split diff view (side-by-side)

### Phase 4: Operations
- [ ] Revert changes
- [ ] Cherry-pick from Stream to Main
- [ ] Squash/combine changes
- [ ] Branch operations (if applicable to Pijul)
- [ ] Create new project from TUI

### Phase 5: Polish
- [ ] Customizable keybindings
- [ ] Theme support
- [ ] Mouse support
- [ ] Async change loading
- [ ] Progress indicators for long operations
- [ ] Configuration file support

## Design Decisions

### Why Ratatui?
- Modern, actively maintained TUI framework
- Excellent documentation
- Flexible layout system
- Rich widget library
- Good crossterm integration

### Why Crossterm?
- Cross-platform terminal support
- Works on Windows, macOS, Linux
- Handles raw mode and alternate screen
- Event-based input handling

### Vim-style Navigation
- Familiar to developers
- Efficient keyboard-driven workflow
- Modal interface reduces key conflicts
- Easy to extend with new commands

### Projects-First Design
- Users need to see all their tracked projects
- Select project before viewing its changes
- Clear context of which project is active
- Enables future multi-project workflows

### Two-Level Tab Hierarchy
- Main tabs (Projects, Channels) for high-level navigation
- Channel sub-tabs (Stream, Main) for channel-specific views
- `Alt+P` / `Alt+C` provide quick access to main tabs from anywhere
- Arrow keys (`←` / `→`) and brackets (`[` / `]`) for channel navigation

## Testing Strategy

### Unit Tests
- Event conversion functions
- App state transitions
- Command parsing
- Tab navigation logic

### Integration Tests
- Full TUI render cycles
- Keyboard input sequences
- Mode transitions
- ProjectManager integration

### Manual Testing
- Visual inspection of layouts
- Cross-terminal compatibility
- Edge cases (small terminals, resize)
- Empty states (no projects, no changes)

## Data Flow

```
                                    ProjectManager
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Izev TUI                                │
│  ┌─────────────────┐    ┌─────────────────────────────────────┐ │
│  │  Projects Tab   │───▶│  Channels Tab                       │ │
│  │  (list all)     │    │  ├─ Stream (auto changes)           │ │
│  │                 │    │  └─ Main (checkpoints, future)      │ │
│  └─────────────────┘    └─────────────────────────────────────┘ │
│      ▲                           ▲                               │
│   Alt+P                       Alt+C                              │
└─────────────────────────────────────────────────────────────────┘
```

## Related Documentation

- [Ize Architecture](../architecture.md)
- [Pijul Backend](pijul-backend-opcode-recording-backend-rework.md)
- [End-to-end Testing](end-to-end-testing.md)
- [Project Manager](../crates/ize-lib/src/project/manager.rs)