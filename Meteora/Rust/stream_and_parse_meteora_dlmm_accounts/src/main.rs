use {
    backoff::{future::retry, ExponentialBackoff},
    clap::Parser as ClapParser,
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    log::{error, info},
    meteora_dlmm_interface::accounts::{
        BinArray, BinArrayAccount, BinArrayBitmapExtension, BinArrayBitmapExtensionAccount, LbPair,
        LbPairAccount, Oracle, OracleAccount, Position, PositionAccount, PositionV2,
        PositionV2Account, PresetParameter, PresetParameterAccount, BIN_ARRAY_ACCOUNT_DISCM,
        BIN_ARRAY_BITMAP_EXTENSION_ACCOUNT_DISCM, LB_PAIR_ACCOUNT_DISCM, ORACLE_ACCOUNT_DISCM,
        POSITION_ACCOUNT_DISCM, POSITION_V2_ACCOUNT_DISCM, PRESET_PARAMETER_ACCOUNT_DISCM,
    },
    serde::Serialize,
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashMap, env, sync::Arc, time::Duration},
    tokio::sync::Mutex,
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_request_filter_accounts_filter,
            subscribe_request_filter_accounts_filter_memcmp, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
        },
        prelude::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
        },
    },
};

type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;

const DLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const METEORA_DLMM_POOL_SIZE: u64 = 904;
pub const MINT_X_OFFSET: u64 = 88; //加上8bytes的discriminator
pub const MINT_Y_OFFSET: u64 = 120;
pub const MINT_X: Pubkey = Pubkey::from_str_const("So11111111111111111111111111111111111111112");
pub const MINT_Y: Pubkey = Pubkey::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
pub const MINT_X_DECIMALS: i32 = 9;
pub const MINT_Y_DECIMALS: i32 = 6;

#[derive(Debug, Clone, ClapParser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, help = "gRPC endpoint")]
    endpoint: String,

    #[clap(long, help = "X-Token")]
    x_token: String,
}

impl Args {
    async fn connect(&self) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        GeyserGrpcClient::build_from_shared(self.endpoint.clone())?
            .x_token(Some(self.x_token.clone()))?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(1024 * 1024 * 1024)
            .connect()
            .await
            .map_err(Into::into)
    }

    pub fn get_txn_updates(&self) -> anyhow::Result<SubscribeRequest> {
        let mut accounts: AccountFilterMap = HashMap::new();

        accounts.insert(
            "accountData".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![DLMM_PROGRAM_ID.to_string()],
                nonempty_txn_signature: None,
                filters: vec![
                    SubscribeRequestFilterAccountsFilter {
                        filter: Some(subscribe_request_filter_accounts_filter::Filter::Datasize(
                            METEORA_DLMM_POOL_SIZE,
                        )),
                    },
                    SubscribeRequestFilterAccountsFilter {
                        filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
                            SubscribeRequestFilterAccountsFilterMemcmp {
                                offset: MINT_X_OFFSET,
                                data: Some(
                                    subscribe_request_filter_accounts_filter_memcmp::Data::Base58(
                                        MINT_X.to_string(),
                                    ),
                                ),
                            },
                        )),
                    },
                    SubscribeRequestFilterAccountsFilter {
                        filter: Some(subscribe_request_filter_accounts_filter::Filter::Memcmp(
                            SubscribeRequestFilterAccountsFilterMemcmp {
                                offset: MINT_Y_OFFSET,
                                data: Some(
                                    subscribe_request_filter_accounts_filter_memcmp::Data::Base58(
                                        MINT_Y.to_string(),
                                    ),
                                ),
                            },
                        )),
                    },
                ],
            },
        );

        Ok(SubscribeRequest {
            accounts,
            slots: HashMap::default(),
            transactions: HashMap::default(),
            transactions_status: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            entry: HashMap::default(),
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        })
    }
}

pub trait AccountData: std::fmt::Debug {}

#[derive(Debug, Default)]
pub struct EmptyAccount;

impl AccountData for EmptyAccount {}

#[derive(Debug, Serialize)]
pub enum DecodedAccount {
    BinArrayBitmapExtension(BinArrayBitmapExtension),
    BinArray(BinArray),
    LbPair(LbPair),
    Oracle(Oracle),
    Position(Position),
    PositionV2(PositionV2),
    PresetParameter(PresetParameter),
}

#[derive(Debug)]
pub struct AccountDecodeError {
    pub message: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);

        async move {
            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let client = args.connect().await.map_err(backoff::Error::transient)?;
            info!("Connected");

            let request = args.get_txn_updates().map_err(backoff::Error::Permanent)?;

            geyser_subscribe(client, request)
                .await
                .map_err(backoff::Error::transient)?;

            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl Interceptor>,
    request: SubscribeRequest,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    info!("stream opened");

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Account(account)) => {
                    let slot = account.slot;
                    //let account_update = account.account;
                    if let Some(account_data) = account.account {
                        let pubkey_str = bs58::encode(&account_data.pubkey).into_string();
                        let owner = bs58::encode(&account_data.owner).into_string();
                        let lamports = account_data.lamports;
                        let executable = account_data.executable;

                        let decoded_account = match decode_account_data(&account_data.data) {
                            Ok(data) => data,
                            Err(e) => {
                                eprintln!("Failed to decode account data: {}", e.message);
                                //return Ok(());// Handle the error as needed
                                continue;
                            }
                        };

                        let account_info = serde_json::json!({
                            "pubkey": pubkey_str,
                            "lamports": lamports,
                            "owner": owner,
                            "executable": executable,
                            "slot": slot,
                            "decoded_data": decoded_account
                        });

                        // println!("\nAccount Info: {}", account_info);
                    } else {
                        println!("Account data is None for slot: {}", slot);
                    }
                }
                Some(UpdateOneof::Ping(_)) => {
                    subscribe_tx
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await?;
                }
                Some(UpdateOneof::Pong(_)) => {}
                None => {
                    error!("update not found in the message");
                    break;
                }
                _ => {}
            },
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }
    }

    info!("stream closed");
    Ok(())
}

pub fn from_x64(x64: u128) -> f64 {
    (x64 as f64) / ((1u128 << 64) as f64)
}

pub fn decode_account_data(buf: &[u8]) -> Result<DecodedAccount, AccountDecodeError> {
    if buf.len() < 8 {
        return Err(AccountDecodeError {
            message: "Buffer too short to contain a valid discriminator.".to_string(),
        });
    }

    let discriminator: [u8; 8] = buf[..8]
        .try_into()
        .expect("Failed to extract first 8 bytes");

    match discriminator {
        BIN_ARRAY_BITMAP_EXTENSION_ACCOUNT_DISCM => {
            //println!("CIN Array Ext Account detected. Proceeding with deserialization...");
            let data = BinArrayBitmapExtensionAccount::deserialize(buf).map_err(|e| {
                AccountDecodeError {
                    message: format!("Failed to deserialize BinArrayExtAccout: {}", e),
                }
            })?;
            println!("\nDecoded Bin Array Ext Structure: {:#?}", data);
            Ok(DecodedAccount::BinArrayBitmapExtension(data.0)) // Unwrapping the inner BondingCurve struct
        }
        BIN_ARRAY_ACCOUNT_DISCM => {
            //println!("BIN Array Account detected. Proceeding with deserialization...");
            let data = BinArrayAccount::deserialize(buf).map_err(|e| AccountDecodeError {
                message: format!("Failed to deserialize BondingCurveAccount: {}", e),
            })?;
            println!("\nDecoded Bonding Curve Structure: {:#?}", data);
            Ok(DecodedAccount::BinArray(data.0))
        }
        LB_PAIR_ACCOUNT_DISCM => {
            //println!("LB Pair Account detected. Proceeding with deserialization...");
            let data = LbPairAccount::deserialize(buf).map_err(|e| AccountDecodeError {
                message: format!("Failed to deserialize BondingCurveAccount: {}", e),
            })?;
            // println!("\nDecoded Bonding Curve Structure: {:#?}", data);
            let raw_price = from_x64(data.get_current_price().unwrap());
            let adjusted_price =
                raw_price * 10f64.powi(MINT_X_DECIMALS) / 10f64.powi(MINT_Y_DECIMALS);
            println!("\nSOL/USDC 当前价格: {:#?}", adjusted_price);
            println!("LB_PAIR_ACCOUNT");
            Ok(DecodedAccount::LbPair(data.0))
        }
        ORACLE_ACCOUNT_DISCM => {
            //println!("Oracle Account detected. Proceeding with deserialization...");
            let data = OracleAccount::deserialize(buf).map_err(|e| AccountDecodeError {
                message: format!("Failed to deserialize BondingCurveAccount: {}", e),
            })?;
            println!("\nDecoded Bonding Curve Structure: {:#?}", data);
            Ok(DecodedAccount::Oracle(data.0))
        }
        POSITION_ACCOUNT_DISCM => {
            //println!("Position Account detected. Proceeding with deserialization...");
            let data = PositionAccount::deserialize(buf).map_err(|e| AccountDecodeError {
                message: format!("Failed to deserialize BondingCurveAccount: {}", e),
            })?;
            println!("\nDecoded Bonding Curve Structure: {:#?}", data);
            Ok(DecodedAccount::Position(data.0))
        }
        _ => Err(AccountDecodeError {
            message: "Account discriminator not found.".to_string(),
        }),
    }
}
