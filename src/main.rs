use anyhow::{Context, Result};
use bincode;
use bs58;
use clap::Parser;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::RpcLeaderSchedule;
use solana_program::vote::instruction::VoteInstruction;
use solana_program::vote::state::Vote;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(
    name = "Vote Checker",
    about = "Check vote transactions by slot/account"
)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long, num_args = 1.., value_delimiter = ',')]
    account: Vec<String>,
    #[arg(long)]
    accounts: Option<String>,
    #[arg(long)]
    slot: Option<u64>,
    #[arg(long)]
    distance: Option<u64>,
    #[arg(long)]
    range: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlockResponse {
    result: Option<BlockResult>,
}

#[derive(Debug, Deserialize)]
struct BlockResult {
    transactions: Vec<Transaction>,
}

#[derive(Debug, Deserialize)]
struct Transaction {
    transaction: TransactionData,
    meta: Option<TransactionMeta>,
}

#[derive(Debug, Deserialize)]
struct TransactionData {
    signatures: Vec<String>,
    message: TransactionMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionMessage {
    account_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionMeta {
    log_messages: Option<Vec<String>>,
}

async fn get_block_with_retry(
    client: &Client,
    api_url: &str,
    slot: u64,
    max_attempts: usize,
) -> Result<Option<BlockResult>> {
    let mut attempts = 0;
    let mut delay = Duration::from_secs(3);
    let mut not_found_lines = 0;

    loop {
        let res = get_block(client, api_url, slot).await;
        match &res {
            Ok(Some(_)) => {
                if not_found_lines > 0 {
                    for _ in 0..not_found_lines {
                        print!("\x1b[1A\x1b[2K");
                    }
                    print!("\r");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
                return res;
            }
            Ok(None) => {
                if not_found_lines > 0 {
                    for _ in 0..not_found_lines {
                        print!("\x1b[1A\x1b[2K");
                    }
                    print!("\r");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
                return res;
            }
            Err(e) => {
                let err_str = e.to_string();
                let is_rate_limited = err_str.contains("429");
                if attempts < max_attempts {
                    attempts += 1;
                    if is_rate_limited {
                        eprintln!(
                            "{} Retrying in {:?}... (attempt {}/{})",
                            "Rate limited (429).".yellow(),
                            delay,
                            attempts,
                            max_attempts
                        );
                    } else {
                        eprintln!(
                            "Error fetching block {}: {}. Retrying in {:?}...",
                            slot, e, delay
                        );
                    }
                    not_found_lines += 1;
                    sleep(delay).await;
                    delay *= 2;
                    continue;
                } else {
                    if not_found_lines > 0 {
                        for _ in 0..not_found_lines {
                            print!("\x1b[1A\x1b[2K");
                        }
                        print!("\r");
                        std::io::Write::flush(&mut std::io::stdout()).unwrap();
                    }
                    return res;
                }
            }
        }
    }
}

async fn get_leader_map_with_retry(
    rpc_url: &str,
    slot: u64,
    max_attempts: usize,
) -> anyhow::Result<HashMap<u64, String>> {
    let mut attempts = 0;
    let mut delay = Duration::from_secs(3);
    let mut rate_limit_lines = 0;

    loop {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner} {msg}")
                .unwrap(),
        );
        pb.set_message("Fetching leader schedule...");
        pb.enable_steady_tick(Duration::from_millis(80));

        let rpc_client = RpcClient::new_with_timeout(rpc_url.to_string(), Duration::from_secs(20));
        let result = map_leader_slots(&rpc_client, slot);

        pb.finish_and_clear();

        match result {
            Ok(map) => {
                if rate_limit_lines > 0 {
                    for _ in 0..rate_limit_lines {
                        print!("\x1b[1A\x1b[2K");
                    }
                    print!("\r");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
                return Ok(map);
            }
            Err(e) => {
                let is_rate_limited = e.to_string().contains("429")
                    || e.to_string().contains("rate limit")
                    || e.to_string().contains("timed out");
                if attempts < max_attempts && is_rate_limited {
                    attempts += 1;
                    eprintln!(
                        "Leader schedule fetch rate limited or timed out. Retrying in {:?}... (attempt {}/{})",
                        delay, attempts, max_attempts
                    );
                    rate_limit_lines += 1;
                    sleep(delay).await;
                    delay *= 2;
                    continue;
                } else {
                    if rate_limit_lines > 0 {
                        for _ in 0..rate_limit_lines {
                            print!("\x1b[1A\x1b[2K");
                        }
                        print!("\r");
                        std::io::Write::flush(&mut std::io::stdout()).unwrap();
                    }
                    return Err(e).context("Failed to fetch leader schedule after retries");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Collect all accounts from command line and JSON file
    let mut all_accounts = args.account.clone();
    
    if let Some(accounts_file) = &args.accounts {
        let file_content = std::fs::read_to_string(accounts_file)
            .context(format!("Failed to read accounts file: {}", accounts_file))?;
        
        let file_accounts: Vec<String> = serde_json::from_str(&file_content)
            .context(format!("Failed to parse JSON from accounts file: {}", accounts_file))?;
        
        all_accounts.extend(file_accounts);
    }

    // Remove duplicates while preserving order
    let mut seen = std::collections::HashSet::new();
    let mut unique_accounts = Vec::new();
    for account in all_accounts {
        if seen.insert(account.clone()) {
            unique_accounts.push(account);
        }
    }

    if unique_accounts.is_empty() {
        anyhow::bail!("No accounts specified. Use --account or --accounts to specify at least one account.");
    }

    let slot_info = if args.range.is_some() {
        format!("Range file: {}", args.range.as_ref().unwrap())
    } else {
        let slot = args.slot.unwrap_or(0);
        let distance = args.distance.unwrap_or(0);
        format!("Slot: {} Distance: {}", slot, distance)
    };

    println!(
        "\n{}\n{}  {} {}\n{}\n",
        "==============================".bright_black(),
        slot_info.bold(),
        "Accounts:".bold(),
        unique_accounts.len().to_string().yellow(),
        "==============================".bright_black()
    );

    // Determine which slots to process
    let slots_to_process: Vec<u64> = if let Some(range_file) = &args.range {
        // Load slots from JSON file
        let file_content = std::fs::read_to_string(range_file)
            .context(format!("Failed to read range file: {}", range_file))?;
        
        let slots: Vec<u64> = serde_json::from_str(&file_content)
            .context(format!("Failed to parse JSON from range file: {}", range_file))?;
        
        slots
    } else {
        // Use original slot + distance logic
        let slot = args.slot.ok_or_else(|| anyhow::anyhow!("--slot is required when not using --range"))?;
        let distance = args.distance.ok_or_else(|| anyhow::anyhow!("--distance is required when not using --range"))?;
        
        let mut slots = Vec::new();
        for offset in 0..=distance {
            slots.push(slot.saturating_sub(offset));
        }
        slots
    };

    // Use the first slot for leader schedule (or a default if using range)
    let reference_slot = if let Some(range_file) = &args.range {
        // Load first slot from range file for leader schedule
        let file_content = std::fs::read_to_string(range_file)
            .context(format!("Failed to read range file: {}", range_file))?;
        
        let slots: Vec<u64> = serde_json::from_str(&file_content)
            .context(format!("Failed to parse JSON from range file: {}", range_file))?;
        
        *slots.first().ok_or_else(|| anyhow::anyhow!("Range file is empty"))?
    } else {
        args.slot.ok_or_else(|| anyhow::anyhow!("--slot is required when not using --range"))?
    };

    let leader_map = get_leader_map_with_retry(&args.url, reference_slot, 5)
        .await
        .context("Could not fetch leader schedule (rate limited or RPC error). Exiting.")?;

    let http_client = Client::new();
    
    // Track missed votes for each account
    let mut missed_votes_count: HashMap<String, u32> = HashMap::new();
    let total_slots = slots_to_process.len();

    for current_slot in slots_to_process {

        let block_result = get_block_with_retry(&http_client, &args.url, current_slot, 5).await;

        match block_result {
            Ok(Some(block)) => {
                let vote_txs = extract_vote_transactions(&block);
                let vote_count = vote_txs.len();

                let mut matches = vec![];
                let mut found_accounts = std::collections::HashSet::new();
                for (i, tx) in vote_txs.iter().enumerate() {
                    if let Some(account) = tx.transaction.message.account_keys.get(0) {
                        if unique_accounts.contains(account) {
                            matches.push((i, tx, account.clone()));
                            found_accounts.insert(account.clone());
                        }
                    }
                }



                let leader_info = leader_map
                    .get(&current_slot)
                    .map(|l| format!("{}", l))
                    .unwrap_or_else(|| "unknown".to_string());

                println!(
                    "\n{:<7} {:<10} {:<7} {:<6} {:<8} {}",
                    "Slot:".bold(),
                    current_slot.to_string().green(),
                    "Votes:".bold(),
                    vote_count.to_string().cyan(),
                    "Leader:".bold(),
                    leader_info.bright_black()
                );

                // Show all accounts with their status and track missed votes
                let mut missed_in_slot = Vec::new();
                for account in &unique_accounts {
                    if found_accounts.contains(account) {
                        // COMMENTED OUT: Successful vote display
                        // Find the position for this account
                        // if let Some((position, _, _)) = matches.iter().find(|(_, tx, acc)| acc == account) {
                        //     println!("  {} {}", account, position.to_string().bright_blue());
                        // }
                    } else {
                        println!("  {} {}", account, "[X]".red());
                        missed_in_slot.push(account.clone());
                        *missed_votes_count.entry(account.clone()).or_insert(0) += 1;
                    }
                }
                
                // Output missed votes summary for this slot
                if !missed_in_slot.is_empty() {
                    println!("  {} missed votes: {}", "Missed:".bold().red(), missed_in_slot.len());
                    println!("  {}", missed_in_slot.join(", ").red());
                }
            }
            Ok(None) => {
                let leader_info = leader_map
                    .get(&current_slot)
                    .map(|l| format!("{}", l.bright_black()))
                    .unwrap_or_else(|| "unknown".to_string());

                println!(
                    "\n{} {}, {} {} {}",
                    "Warning:".bold().yellow(),
                    format!("No block found for {}", current_slot),
                    "likely a skipped/non-canonical slot, or RPC did not respond.".dimmed(),
                    "Leader:".bold(),
                    leader_info
                );
                sleep(Duration::from_millis(300)).await;
            }
            Err(e) => {
                let leader_info = leader_map
                    .get(&current_slot)
                    .map(|l| format!("{}", l.bright_black()))
                    .unwrap_or_else(|| "unknown".to_string());

                println!(
                    "\n{} {}, {} {} {} ({})",
                    "Warning:".bold().yellow(),
                    format!("No block found for {}", current_slot),
                    "likely a skipped/non-canonical slot, or RPC did not respond.".dimmed(),
                    "Leader:".bold(),
                    leader_info,
                    e
                );
            }
        }

        let mut rng = rand::rng();
        let jitter = rng.random_range(300..=600);
        sleep(Duration::from_millis(jitter)).await;
    }

    println!("{}", "\nAll done!".bright_green());
    
    // Output final missed votes summary
    println!("\n{}", "=".repeat(60).bright_black());
    println!("{}", "FINAL SUMMARY".bold());
    println!("{}", "=".repeat(60).bright_black());
    println!("  Total slots processed: {}", total_slots.to_string().cyan());
    
    if !missed_votes_count.is_empty() {
        println!("\n{}", "MISSED VOTES SUMMARY".bold().red());
        
        // Sort by missed vote count (descending)
        let mut sorted_missed: Vec<_> = missed_votes_count.iter().collect();
        sorted_missed.sort_by(|a, b| b.1.cmp(a.1));
        
        for (account, count) in sorted_missed {
            println!("  {}: {} missed votes", account, count.to_string().red());
        }
    } else {
        println!("\n{}", "No missed votes found!".bold().green());
    }
    
    println!("{}", "=".repeat(60).bright_black());
    
    println!();
    Ok(())
}

fn extract_vote_transactions(block: &BlockResult) -> Vec<&Transaction> {
    block
        .transactions
        .iter()
        .filter(|tx| {
            tx.meta
                .as_ref()
                .and_then(|meta| meta.log_messages.as_ref())
                .map_or(false, |logs| {
                    logs.iter().any(|log| {
                        log.starts_with(
                            "Program Vote111111111111111111111111111111111111111 invoke",
                        )
                    })
                })
        })
        .collect()
}

async fn get_block(client: &Client, api_url: &str, slot: u64) -> Result<Option<BlockResult>> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 0
            }
        ]
    });

    let resp = client
        .post(api_url)
        .json(&body)
        .send()
        .await
        .context("Failed to send getBlock request")?;

    let value = resp
        .json::<serde_json::Value>()
        .await
        .context("Failed to parse getBlock response")?;

    if let Some(error) = value.get("error") {
        let code = error.get("code").and_then(|c| c.as_i64());
        if code == Some(-32009) || code == Some(-32007) {
            return Ok(None);
        } else if code == Some(429) {
            anyhow::bail!("429");
        } else {
            let msg = error.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            if let Some(code) = code {
                anyhow::bail!("error {}: {}", code, msg);
            } else {
                anyhow::bail!("{}", msg);
            }
        }
    }

    let block_resp: BlockResponse = serde_json::from_value(value)
        .context("Failed to parse getBlock response as BlockResponse")?;

    Ok(block_resp.result)
}

async fn extract_voted_slot(rpc_url: &str, signature: &str) -> Result<Option<u64>> {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "json",
                "commitment": "confirmed"
            }
        ]
    });

    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("Failed to send getTransaction request")?
        .json::<serde_json::Value>()
        .await
        .context("Failed to parse getTransaction response")?;

    if let Some(error) = resp.get("error") {
        if error.get("code") == Some(&serde_json::json!(429)) {
            anyhow::bail!("429");
        }
    }

    let print_debug = |reason: &str| {
        eprintln!(
            "DEBUG: Could not extract voted slot for signature {} ({}). Full transaction JSON:\n{}",
            signature,
            reason,
            serde_json::to_string_pretty(&resp)
                .unwrap_or_else(|_| "<failed to serialize>".to_string())
        );
    };

    let tx = match resp
        .get("result")
        .and_then(|r| r.get("transaction"))
        .and_then(|t| t.get("message"))
    {
        Some(tx) => tx,
        None => {
            print_debug("missing transaction/message");
            return Ok(None);
        }
    };
    let instructions = match tx.get("instructions").and_then(|i| i.as_array()) {
        Some(i) => i,
        None => {
            print_debug("missing instructions");
            return Ok(None);
        }
    };
    let account_keys = match tx.get("accountKeys").and_then(|a| a.as_array()) {
        Some(a) => a,
        None => {
            print_debug("missing accountKeys");
            return Ok(None);
        }
    };

    for instr in instructions {
        let program_index = match instr.get("programIdIndex").and_then(|i| i.as_u64()) {
            Some(idx) => idx as usize,
            None => {
                print_debug("missing programIdIndex");
                continue;
            }
        };
        let program_id = match account_keys.get(program_index).and_then(|k| k.as_str()) {
            Some(pid) => pid,
            None => {
                print_debug("missing program_id in account_keys");
                continue;
            }
        };

        if program_id != "Vote111111111111111111111111111111111111111" {
            continue;
        }

        let encoded_data = match instr.get("data").and_then(|d| d.as_str()) {
            Some(data) => data,
            None => {
                print_debug("missing data in instruction");
                continue;
            }
        };
        let decoded_data = match bs58::decode(encoded_data).into_vec() {
            Ok(d) => d,
            Err(err) => {
                eprintln!("Failed to decode base58: {}", err);
                print_debug("base58 decode failed");
                continue;
            }
        };

        match bincode::deserialize::<VoteInstruction>(&decoded_data) {
            Ok(VoteInstruction::Vote(vote_tx)) => {
                let vote: &Vote = &vote_tx;
                if let Some((slot, _)) = vote
                    .slots
                    .iter()
                    .zip((1..=vote.slots.len()).rev())
                    .find(|(_, confirmation_count)| *confirmation_count == 1)
                {
                    return Ok(Some(*slot));
                }
            }
            Ok(VoteInstruction::TowerSync(sync)) => {
                if let Some(lockout) = sync.lockouts.iter().find(|l| l.confirmation_count() == 1) {
                    return Ok(Some(lockout.slot()));
                }
            }
            Ok(other) => {
                eprintln!("Decoded but not Vote or TowerSync: {:?}", other);
                print_debug("decoded but not Vote or TowerSync");
            }
            Err(err) => {
                eprintln!("Failed to deserialize vote instruction: {}", err);
                print_debug("bincode deserialize failed");
            }
        }
    }

    print_debug("no matching instruction found");
    Ok(None)
}

fn map_leader_slots(client: &RpcClient, slot: u64) -> anyhow::Result<HashMap<u64, String>> {
    let epoch_start = get_epoch_start_slot(client, slot)?;
    let schedule: RpcLeaderSchedule = client
        .get_leader_schedule(Some(epoch_start))?
        .ok_or_else(|| anyhow::anyhow!("No leader schedule found"))?;

    let mut slot_to_leader = HashMap::new();
    for (validator, rel_slots) in schedule {
        for rel_slot in rel_slots {
            let abs_slot = epoch_start + rel_slot as u64;
            slot_to_leader.insert(abs_slot, validator.clone());
        }
    }
    Ok(slot_to_leader)
}

fn get_epoch_start_slot(client: &RpcClient, slot: u64) -> anyhow::Result<u64> {
    let schedule = client.get_epoch_schedule()?;
    let epoch = schedule.get_epoch(slot);
    Ok(schedule.get_first_slot_in_epoch(epoch))
}
