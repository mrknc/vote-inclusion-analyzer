
This is a quick-n-dirty hack to investigate immediate problem with intentional vote skipping.

All the credit for building this goes to original author:
https://github.com/a3mc/vote-inclusion-analyzer

jito200.json - list of identities of top 200 ranked by Stakenet (epoch 825)
slots.json - list of slots to check

example use: 
cargo run -r -- --url $SOLANA_RPC_URL --accounts jito200.json --range slots.json