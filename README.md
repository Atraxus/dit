# Dit
Dit is a university project that builds a peer-to-peer network implementation for Git using the Rust programming language. Leveraging the Chord distributed hash table protocol, it seeks to provide a scalable and efficient solution for distributed version control.


## Steps to test:
 - `cargo install --path dit`
 - `dit --config demo/a/dit-config.toml init`
 - `dit --config demo/b/dit-config.toml init`
 - `For a, set peer.listener to "127.0.0.1:7700"`
 - `For b, set peer.listener to "127.0.0.1:7710" and deamon.listener to "127.0.0.1:7711"`
 - `(cli 2) dit --config demo/a/dit-config.toml daemon`
 - `(cli 3) dit --config demo/b/dit-config.toml daemon`
 - `dit --config demo/a/dit-config.toml bootstrap 127.0.0.1:7710`
 - `dit --config demo/a/dit-config.toml add demo/a/dit-config.toml`
 - `dit --config demo/a/dit-config.toml announce`
 - `dit --config demo/b/dit-config.toml fetch 246063b3f15355bec4a3c5ced98cce5dcaf7bbb8fbf98edbfd257a091434710b`
 - `dit --config demo/b/dit-config.toml cat 246063b3f15355bec4a3c5ced98cce5dcaf7bbb8fbf98edbfd257a091434710b`
