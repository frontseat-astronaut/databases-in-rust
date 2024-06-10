# ddia-in-rust
Things implemented so far:
- Log DB: Simplest DB that stores key, value pairs in a single file.
- Log DB with Hash index: Same DB as above but with an in-memory index that stores file offset for each key.
- Segmented version of the above: This one stores the records over multiple segments, and a background process merges older segments to save disk space.
- [WIP] SSTable: A segmented files database where each segment has entries sorted by keys. This allows us to have a sparser index in memory. That requires us to also maintain an in-memory sorted data structure which stores the most recent entries.

All of the above are key-value stores that support set, get and delete.

To run, 
```
cargo run
```

### Resources
- [Designing Data Intensive Applications](https://dataintensive.net/) by Martin Kleppmann
- [The Rust Programming Language](https://doc.rust-lang.org/book/title-page.html) by Steve Klabnik and Carol Nichols
- [Error Handling in Rust](https://blog.burntsushi.net/rust-error-handling/) by Andrew Gallant
- [u/cameronm1024's comment explaining 'static trait bound](https://www.reddit.com/r/learnrust/comments/12fpu7m/comment/jfgjx2k/?utm_source=share&utm_medium=web3x&utm_name=web3xcss&utm_term=1&utm_content=share_button)
- [Generic associated types](https://blog.rust-lang.org/2022/10/28/gats-stabilization.html) by Jack Huey
- B-trees:
    - https://benjamincongdon.me/blog/2021/08/17/B-Trees-More-Than-I-Thought-Id-Want-to-Know/
    - https://siemens.blog/posts/how-databases-store-and-retrieve-data/ 
    - https://siemens.blog/posts/database-page-layout/

Special thanks to @iGamer0020 for helping me build the test suite!