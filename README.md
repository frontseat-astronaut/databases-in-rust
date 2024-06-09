# ddia-in-rust
Things implemented so far:
- Log DB: Simplest DB that stores key, value pairs in a single file.
- Log DB with Hash index: Same DB as above but with an in-memory index that stores file offset for each key.
- Segmented version of the above: This one stores the records over multiple segments, and a background process merges older segments to save disk space.
- [WIP] SSTable: A segmented files database where each segment has entries sorted by keys. This allows us to have a sparser index in memory. That requires us to also maintain an in-memory sorted data structure which stores the most recent entries.

All of the above are key-value stores that support set, get and delete.

To build,
```
cargo build
```
To run, 
```
cargo run
```

### Resources
- [Designing Data Intensive Applications](https://dataintensive.net/) by Martin Kleppmann
- [The Rust Programming Language](https://doc.rust-lang.org/book/title-page.html) by Steve Klabnik and Carol Nichols
- [Error Handling in Rust](https://blog.burntsushi.net/rust-error-handling/) by Andrew Gallant

Special thanks to @iGamer0020 for helping me build the test suite!