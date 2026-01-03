use crate::models::MatchInfo;
use anyhow::{anyhow, Context, Result};
use chrono::DateTime;
use riven::models::match_v5::Match;
use sqlx::{Row, SqlitePool};
use std::collections::{HashMap, HashSet};

/// Inserts or ignores a match record into the database.
pub async fn update_match(
    pool: &SqlitePool,
    match_id: &str, // Use &str for borrowing
    queue_id: i64,
    is_win: bool,
    end_timestamp: i64,
    match_info: &Match,
) -> Result<()> {
    let match_info_json =
        serde_json::to_string(match_info).context("Failed to serialize match info to JSON")?;

    sqlx::query(
        "INSERT OR IGNORE INTO match (id, queue_id, win, end_timestamp, MatchInfo) VALUES (?, ?, ?, ?, ?);"
    )
    .bind(match_id)
    .bind(queue_id)
    .bind(is_win)
    .bind(end_timestamp)
    .bind(&match_info_json) // Bind JSON string
    .execute(pool)
    .await
    .context("Failed to insert match data into MATCH table")?;

    Ok(())
}

/// Fetches a map of lowercase Summoner Names to Discord IDs from the User table.
pub async fn fetch_discord_usernames(pool: &SqlitePool) -> Result<HashMap<String, String>> {
    // Fetch rows as tuples (String, String)
    let mappings: Vec<(String, String)> =
        sqlx::query_as("SELECT DiscordId, LOWER(summonerName) FROM User")
            .fetch_all(pool) // Fetch all results into a Vec
            .await
            .context("Failed to fetch DiscordId and SummonerName from User table")?;

    // Convert Vec<(DiscordId, LowerSummonerName)> to HashMap<LowerSummonerName, DiscordId>
    let summoner_map: HashMap<String, String> = mappings
        .into_iter()
        .map(|(id, name)| (name, id)) // Flip order for map
        .collect();

    Ok(summoner_map)
}

/// Fetches a set of PUUIDs corresponding to the given lowercase summoner names.
pub async fn fetch_puuids(
    pool: &SqlitePool,
    summoner_names: &HashSet<String>,
) -> Result<HashSet<String>> {
    if summoner_names.is_empty() {
        return Ok(HashSet::new());
    }

    // Convert HashSet to Vec for binding
    let names_vec: Vec<String> = summoner_names.iter().cloned().collect();
    // Serialize the Vec to a JSON string for the IN clause workaround
    let names_json =
        serde_json::to_string(&names_vec).context("Failed to serialize summoner names to JSON")?;

    // Use query_scalar to fetch a single column directly
    // The `json_each(?)` trick allows binding a list to an IN clause in SQLite with sqlx
    let puuids: Vec<String> = sqlx::query_scalar(
        "SELECT Puuid FROM summoner WHERE Name IN (SELECT value FROM json_each(?))",
    )
    .bind(names_json) // Bind the JSON array string
    .fetch_all(pool)
    .await
    .context("Failed to fetch PUUIDs from Summoner table")?;

    // Collect into HashSet
    Ok(puuids.into_iter().collect())
}

/// Fetches a map of summoner names to their LoL and TFT PUUIDs.
pub async fn fetch_summoner_puuids(
    pool: &SqlitePool,
    summoner_names: &HashSet<String>,
) -> Result<HashMap<String, (String, Option<String>)>> {
    if summoner_names.is_empty() {
        return Ok(HashMap::new());
    }

    let names_vec: Vec<String> = summoner_names.iter().cloned().collect();
    let names_json =
        serde_json::to_string(&names_vec).context("Failed to serialize summoner names to JSON")?;

    let rows = sqlx::query(
        "SELECT Name, Puuid, TftPuuid FROM summoner WHERE Name IN (SELECT value FROM json_each(?))",
    )
    .bind(names_json)
    .fetch_all(pool)
    .await
    .context("Failed to fetch PUUIDs from Summoner table")?;

    let mut results = HashMap::new();
    for row in rows {
        let name: String = row.try_get("Name")?;
        let puuid: String = row.try_get("Puuid")?;
        let tft_puuid: Option<String> = row.try_get("TftPuuid")?;
        results.insert(name, (puuid, tft_puuid));
    }

    Ok(results)
}

/// Fetches a set of TFT PUUIDs (falling back to LoL PUUID if TFT is null) for the given summoner names.
pub async fn fetch_tft_puuids(
    pool: &SqlitePool,
    summoner_names: &HashSet<String>,
) -> Result<HashSet<String>> {
    if summoner_names.is_empty() {
        return Ok(HashSet::new());
    }

    let names_vec: Vec<String> = summoner_names.iter().cloned().collect();
    let names_json =
        serde_json::to_string(&names_vec).context("Failed to serialize summoner names to JSON")?;

    let puuids: Vec<String> = sqlx::query_scalar(
        "SELECT COALESCE(TftPuuid, Puuid) FROM summoner WHERE Name IN (SELECT value FROM json_each(?))",
    )
    .bind(names_json)
    .fetch_all(pool)
    .await
    .context("Failed to fetch TFT PUUIDs from Summoner table")?;

    Ok(puuids.into_iter().collect())
}

/// Fetches a map of Discord IDs (as u64) to Summoner Names from the User table.
pub async fn fetch_summoners(pool: &SqlitePool) -> Result<HashMap<u64, String>> {
    // Fetch rows as tuples (String, String)
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT DiscordId, SummonerName FROM USER")
        .fetch_all(pool)
        .await
        .context("Failed to fetch DiscordId and SummonerName from User table")?;

    let mut summoners = HashMap::new();
    for (discord_id_str, summoner_name) in rows {
        // Parse DiscordId String to u64
        let discord_id = discord_id_str
            .parse::<u64>()
            .with_context(|| format!("Failed to parse DiscordId '{}' as u64", discord_id_str))?;
        summoners.insert(discord_id, summoner_name);
    }
    Ok(summoners)
}

/// Fetches information for matches whose IDs are in the provided list.
pub async fn fetch_seen_events(
    pool: &SqlitePool,
    match_ids: &[String], // Use slice for borrowing
) -> Result<HashMap<String, MatchInfo>> {
    if match_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Serialize Vec<String> to JSON for IN clause
    let ids_json =
        serde_json::to_string(match_ids).context("Failed to serialize match IDs to JSON")?;

    // Fetch all rows matching the IDs
    let rows = sqlx::query( "SELECT Id, QUEUE_ID, Win, IFNULL(END_TIMESTAMP, 0) as end_ts, MatchInfo FROM MATCH WHERE Id IN (SELECT value FROM json_each(?))")
        .bind(ids_json)
        .fetch_all(pool)
        .await
        .context("Failed to fetch match data from MATCH table")?;

    let mut seen_matches: HashMap<String, MatchInfo> = HashMap::with_capacity(rows.len());

    // Process each row manually
    for row in rows {
        // Use try_get from SqlxRow trait
        let id: String = row.try_get("Id")?;
        let queue_id_str: String = row.try_get("QUEUE_ID")?; // Fetch as string first
        let win: bool = row.try_get("Win")?;
        let timestamp_ms: i64 = row.try_get("end_ts")?; // Use alias 'end_ts'
        let match_info_str: Option<String> = row.try_get("MatchInfo")?;

        // Parse timestamp
        let end_timestamp = DateTime::from_timestamp_millis(timestamp_ms)
            .ok_or_else(|| anyhow!("Invalid timestamp millis from DB: {}", timestamp_ms))?;

        // Parse MatchInfo JSON string
        let match_info: Option<Match> = match match_info_str {
            Some(s) if !s.is_empty() => match serde_json::from_str(&s) {
                std::result::Result::Ok(m) => Some(m),
                Err(e) => {
                    // Log the error but don't fail the whole function
                    tracing::error!("Failed to parse MatchInfo JSON for match {}: {}", id, e);
                    None
                }
            },
            _ => None,
        };

        // Insert into the result map
        seen_matches.insert(
            id.clone(),
            MatchInfo {
                id,
                queue_id: queue_id_str, // Keep as String as in struct definition
                win,
                end_timestamp,
                match_info,
            },
        );
    }
    Ok(seen_matches)
}

/// Inserts or replaces user and summoner data during registration.
pub async fn register_user_summoner(
    pool: &SqlitePool,
    user: &serenity::model::user::User,
    summoner_name_lower: &str,
    summoner_info: &riven::models::summoner_v4::Summoner,
    tft_puuid: Option<&str>,
) -> Result<()> {
    let discord_id: u64 = user.id.get();
    let discord_username: &str = &user.name;
    let discord_display_name: &str = user.global_name.as_deref().unwrap_or(&user.name);
    let riot_id_or_summoner_name: &str = summoner_name_lower;
    let puuid: &str = &summoner_info.puuid;
    let summoner_id = summoner_info.id.clone().unwrap_or_default();
    // Use a transaction for atomicity
    let mut tx = pool.begin().await.context("Failed to begin transaction")?;

    // Insert or Update User table
    sqlx::query(
        "INSERT OR REPLACE INTO User (DiscordId, SummonerName, DiscordUsername, DiscordDisplayName) VALUES (?, ?, ?, ?);"
    )
    .bind(discord_id.to_string())
    .bind(riot_id_or_summoner_name) // Store the provided name/riot id (lowercase handled by caller if needed)
    .bind(discord_username)
    .bind(discord_display_name)
    .execute(&mut *tx) // Execute within the transaction
    .await
    .context("Failed to insert/update user data into User table")?;

    // Insert or Update Summoner table
    // Using INSERT OR REPLACE based on puuid primary key
    sqlx::query("INSERT OR REPLACE INTO Summoner (puuid, TftPuuid, name, id) VALUES (?, ?, ?, ?);")
        .bind(puuid)
        .bind(tft_puuid)
        .bind(riot_id_or_summoner_name) // Store the provided name/riot id
        .bind(summoner_id)
        .execute(&mut *tx) // Execute within the transaction
        .await
        .context("Failed to insert/update summoner data into Summoner table")?;

    // Commit the transaction
    tx.commit().await.context("Failed to commit transaction")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*; // Import items from outer scope
    use crate::tests::{
        create_test_match, create_test_participant, create_test_team, setup_in_memory_db_pool,
    };
    use chrono::Utc;
    use riven::consts::{Queue, Team as TeamId};
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_fetch_summoners_db_empty() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let summoners = fetch_summoners(&pool).await?;
        assert!(summoners.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_summoners_db_with_data() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        sqlx::query("INSERT INTO User (DiscordId, SummonerName) VALUES (?1, ?2), (?3, ?4)")
            .bind("111")
            .bind("PlayerOne")
            .bind("222")
            .bind("PlayerTwo")
            .execute(&pool)
            .await?;

        let summoners = fetch_summoners(&pool).await?;

        assert_eq!(summoners.len(), 2);
        assert_eq!(summoners.get(&111), Some(&"PlayerOne".to_string()));
        assert_eq!(summoners.get(&222), Some(&"PlayerTwo".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_puuids_db_empty_input() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        // Insert some data to ensure the function handles empty input correctly even if DB has data
        sqlx::query("INSERT INTO summoner (Puuid, name) VALUES ('puuid1', 'playerone')")
            .execute(&pool)
            .await?;

        let summoner_names_to_fetch: HashSet<String> = HashSet::new();
        let puuids = fetch_puuids(&pool, &summoner_names_to_fetch).await?;
        assert!(puuids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_puuids_db_with_data() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        sqlx::query("INSERT INTO summoner (Puuid, name) VALUES (?1, ?2), (?3, ?4), (?5, ?6)")
            .bind("puuid1")
            .bind("playerone") // Keep names lowercase in DB for consistency
            .bind("puuid2")
            .bind("playertwo")
            .bind("puuid3")
            .bind("playerthree")
            .execute(&pool)
            .await?;

        let summoner_names_to_fetch: HashSet<String> = HashSet::from([
            "playerone".to_string(), // Use lowercase to match DB query logic
            "playertwo".to_string(),
            "playerfour".to_string(), // Non-existent
        ]);

        let puuids = fetch_puuids(&pool, &summoner_names_to_fetch).await?;

        assert_eq!(puuids.len(), 2);
        assert!(puuids.contains("puuid1"));
        assert!(puuids.contains("puuid2"));
        assert!(!puuids.contains("puuid3"));
        Ok(())
    }

    #[tokio::test]
    async fn test_update_match_ignore_duplicate() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let now = Utc::now().timestamp_millis();
        let match_id = "NA1_DUPLICATE";
        let queue_id = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO;
        let is_win = true;

        let p1 = create_test_participant(
            "p1",
            "P1#NA1#NA1",
            TeamId::BLUE,
            true,
            1,
            0,
            0,
            0,
            None,
            false,
        );
        let t1 = create_test_team(TeamId::BLUE, true);
        let t2 = create_test_team(TeamId::RED, false);
        // Clone t1 and t2 here as they are used again later
        let test_match = create_test_match(
            match_id,
            queue_id,
            Some(now),
            vec![p1.clone()], // Clone p1 if it might be needed again (though not in this specific test)
            vec![t1.clone(), t2.clone()],
        );

        // Insert first time
        update_match(
            &pool,
            match_id,
            i64::from(u16::from(queue_id)),
            is_win,
            now,
            &test_match,
        )
        .await?;

        // Attempt to insert second time (should be ignored)
        let p1_updated =
            create_test_participant("p1", "P1#NA1", TeamId::BLUE, true, 5, 0, 0, 0, None, false); // Change some data
                                                                                                  // Use the original t1 and t2 (or clones if they were modified)
        let test_match_updated = create_test_match(
            match_id,
            queue_id,
            Some(now), // Use original timestamp for consistency if needed, or now + 1000 if testing timestamp update logic specifically
            vec![p1_updated],
            vec![t1, t2], // Use the original t1 and t2 variables
        );
        update_match(
            &pool,
            match_id,
            i64::from(u16::from(queue_id)),
            false,      // Change win status
            now + 1000, // Change timestamp slightly
            &test_match_updated,
        )
        .await?;

        // Fetch and verify it's the original data
        let fetched: (String, bool, i64, String) =
            sqlx::query_as("SELECT id, win, end_timestamp, MatchInfo FROM match WHERE id = ?")
                .bind(match_id)
                .fetch_one(&pool)
                .await?;

        assert_eq!(fetched.0, match_id);
        assert_eq!(fetched.1, is_win); // Should still be true (original value)
        assert_eq!(fetched.2, now); // Should be original timestamp
        let fetched_match_info: Match = serde_json::from_str(&fetched.3)?;
        assert_eq!(fetched_match_info.info.participants[0].kills, 1); // Original kills

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_seen_events_empty_input() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let match_ids: Vec<String> = Vec::new();
        let seen_matches = fetch_seen_events(&pool, &match_ids).await?;
        assert!(seen_matches.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_seen_events_with_data() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let now = Utc::now().timestamp_millis();
        let match_id1 = "NA1_SEEN1";
        let match_id2 = "NA1_SEEN2";
        let match_id3 = "NA1_NOTSEEN"; // This one won't be inserted
        let queue_id1 = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO;
        let queue_id2 = Queue::HOWLING_ABYSS_5V5_ARAM;

        let p1 =
            create_test_participant("p1", "P1#NA1", TeamId::BLUE, true, 1, 0, 0, 0, None, false);
        let t1 = create_test_team(TeamId::BLUE, true);
        let t2 = create_test_team(TeamId::RED, false);
        let test_match1 = create_test_match(
            match_id1,
            queue_id1,
            Some(now),
            vec![p1.clone()],
            vec![t1.clone(), t2.clone()],
        );
        let test_match2 = create_test_match(
            match_id2,
            queue_id2,
            Some(now - 10000),
            vec![p1],     // p1 is moved here
            vec![t1, t2], // t1 and t2 are moved here
        );

        // Insert match 1
        update_match(
            &pool,
            match_id1,
            i64::from(u16::from(queue_id1)),
            true,
            now,
            &test_match1,
        )
        .await?;
        // Insert match 2
        update_match(
            &pool,
            match_id2,
            i64::from(u16::from(queue_id2)),
            false,
            now - 10000,
            &test_match2,
        )
        .await?;
        // Insert a match with NULL MatchInfo (by inserting manually)
        sqlx::query("INSERT INTO match (id, queue_id, win, end_timestamp, MatchInfo) VALUES (?, ?, ?, ?, NULL)")
            .bind("NA1_NULL_INFO")
            .bind(i64::from(u16::from(Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK)))
            .bind(true)
            .bind(now - 20000)
            .execute(&pool)
            .await?;

        let matches_to_fetch = vec![
            match_id1.to_string(),
            match_id3.to_string(),
            "NA1_NULL_INFO".to_string(),
        ];
        let seen_matches_map = fetch_seen_events(&pool, &matches_to_fetch).await?;

        assert_eq!(seen_matches_map.len(), 2); // Should only find the two inserted matches

        // Check match 1
        let fetched_info1 = seen_matches_map.get(match_id1).unwrap();
        assert_eq!(fetched_info1.id, match_id1);
        assert_eq!(
            fetched_info1.queue_id,
            i64::from(u16::from(queue_id1)).to_string()
        );
        assert_eq!(fetched_info1.win, true);
        assert_eq!(fetched_info1.end_timestamp.timestamp_millis(), now);
        assert!(fetched_info1.match_info.is_some());
        assert_eq!(
            fetched_info1.match_info.as_ref().unwrap().metadata.match_id,
            match_id1
        );

        // Check match with NULL info
        let fetched_info_null = seen_matches_map.get("NA1_NULL_INFO").unwrap();
        assert_eq!(fetched_info_null.id, "NA1_NULL_INFO");
        assert_eq!(
            fetched_info_null.queue_id,
            i64::from(u16::from(Queue::SUMMONERS_RIFT_5V5_DRAFT_PICK)).to_string()
        );
        assert_eq!(fetched_info_null.win, true);
        assert_eq!(
            fetched_info_null.end_timestamp.timestamp_millis(),
            now - 20000
        );
        assert!(fetched_info_null.match_info.is_none()); // MatchInfo should be None

        // Check that the non-existent match wasn't found
        assert!(seen_matches_map.get(match_id3).is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_discord_usernames_db_empty() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let username_map = fetch_discord_usernames(&pool).await?;
        assert!(username_map.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_discord_usernames_db_with_data() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        sqlx::query("INSERT INTO User (DiscordId, SummonerName, DiscordUsername) VALUES (?1, ?2, ?3), (?4, ?5, ?6)")
            .bind("111")
            .bind("PlayerOne") // Mixed case
            .bind("discord_user1")
            .bind("222")
            .bind("playertwo") // Lower case
            .bind("discord_user2")
            .execute(&pool)
            .await?;

        let username_map = fetch_discord_usernames(&pool).await?;

        assert_eq!(username_map.len(), 2);
        assert_eq!(username_map.get("playerone"), Some(&"111".to_string())); // Should be lowercase
        assert_eq!(username_map.get("playertwo"), Some(&"222".to_string()));
        assert!(username_map.get("PlayerOne").is_none()); // Original case should not exist

        Ok(())
    }

    #[tokio::test]
    async fn test_register_user_summoner_db() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let discord_id = 123456789u64;
        let discord_username = "TestUser";
        let discord_display_name = "Test Display Name";
        let riot_id = "TestRiot#NA1";
        let puuid = "test_puuid_123";
        let summoner_id = "test_summoner_id_456";

        // Initial registration
        register_user_summoner(
            &pool,
            &serde_json::from_str(
                r#"{
                "username": "TestUser",
                "id": 123456789,
                "global_name": "Test Display Name"
            }"#,
            )
            .unwrap(), // Placeholder
            riot_id,
            &serde_json::from_str(
                r#"{
                "puuid": "test_puuid_123",
                "id": "test_summoner_id_456",
                "profileIconId": 1234,
                "revisionDate": 1234567890,
                "summonerLevel": 30
            }"#,
            )
            .unwrap(),
            None,
        )
        .await?;

        // Verify User table entry
        let user_row = sqlx::query("SELECT DiscordId, SummonerName, DiscordUsername, DiscordDisplayName FROM User WHERE DiscordId = ?")
            .bind(discord_id.to_string())
            .fetch_one(&pool)
            .await?;
        let fetched_discord_id: String = user_row.try_get("DiscordId")?;
        let fetched_summoner_name: String = user_row.try_get("SummonerName")?;
        let fetched_discord_username: String = user_row.try_get("DiscordUsername")?;
        let fetched_discord_display_name: String = user_row.try_get("DiscordDisplayName")?;

        assert_eq!(fetched_discord_id, discord_id.to_string());
        assert_eq!(fetched_summoner_name, riot_id);
        assert_eq!(fetched_discord_username, discord_username);
        assert_eq!(fetched_discord_display_name, discord_display_name);

        // Verify Summoner table entry
        let summoner_row =
            sqlx::query("SELECT Puuid, TftPuuid, Name, Id FROM Summoner WHERE Puuid = ?")
                .bind(puuid)
                .fetch_one(&pool)
                .await?;
        let fetched_puuid: String = summoner_row.try_get("Puuid")?;
        let fetched_tft_puuid: Option<String> = summoner_row.try_get("TftPuuid")?;
        let fetched_name: String = summoner_row.try_get("Name")?;
        let fetched_id: String = summoner_row.try_get("Id")?;

        assert_eq!(fetched_puuid, puuid);
        assert_eq!(fetched_tft_puuid, None);
        assert_eq!(fetched_name, riot_id);
        assert_eq!(fetched_id, summoner_id);

        let updated_riot_id = "UpdatedRiot#NA1";
        let updated_summoner_id = "updated_summoner_id";
        let updated_discord_username = "UpdatedUsername";
        let updated_discord_display_name = "Updated Display";

        register_user_summoner(
            &pool,
            &serde_json::from_str(
                r#"{
                "username": "UpdatedUsername",
                "id": 123456789,
                "global_name": "Updated Display"
            }"#,
            )
            .unwrap(),
            updated_riot_id,
            &serde_json::from_str(
                r#"{
                "id": "updated_summoner_id",
                "puuid": "test_puuid_123",
                "profileIconId": 1234,
                "revisionDate": 1234567890,
                "summonerLevel": 30
            }"#,
            )
            .unwrap(),
            Some("test_tft_puuid"),
        )
        .await?;

        // Verify User table update
        let updated_user_row = sqlx::query("SELECT SummonerName, DiscordUsername, DiscordDisplayName FROM User WHERE DiscordId = ?")
            .bind(discord_id.to_string())
            .fetch_one(&pool)
            .await?;
        let updated_fetched_summoner_name: String = updated_user_row.try_get("SummonerName")?;
        let updated_fetched_discord_username: String =
            updated_user_row.try_get("DiscordUsername")?;
        let updated_fetched_discord_display_name: String =
            updated_user_row.try_get("DiscordDisplayName")?;

        assert_eq!(updated_fetched_summoner_name, updated_riot_id);
        assert_eq!(updated_fetched_discord_username, updated_discord_username);
        assert_eq!(
            updated_fetched_discord_display_name,
            updated_discord_display_name
        );

        // Verify Summoner table update
        let updated_summoner_row =
            sqlx::query("SELECT Name, TftPuuid, Id FROM Summoner WHERE Puuid = ?")
                .bind(puuid)
                .fetch_one(&pool)
                .await?;
        let updated_fetched_name: String = updated_summoner_row.try_get("Name")?;
        let updated_fetched_tft_puuid: Option<String> = updated_summoner_row.try_get("TftPuuid")?;
        let updated_fetched_id: String = updated_summoner_row.try_get("Id")?;

        assert_eq!(updated_fetched_name, updated_riot_id);
        assert_eq!(
            updated_fetched_tft_puuid,
            Some("test_tft_puuid".to_string())
        );
        assert_eq!(updated_fetched_id, updated_summoner_id);

        Ok(())
    }

    // Renamed the original combined test for clarity
    #[tokio::test]
    async fn test_update_and_fetch_match_db_basic() -> Result<()> {
        let pool = setup_in_memory_db_pool().await?;
        let now = Utc::now().timestamp_millis();
        let match_id = "NA1_TESTMATCH1".to_string();
        let queue_id = Queue::SUMMONERS_RIFT_5V5_RANKED_SOLO;
        let is_win = true;

        let participant1 = create_test_participant(
            "puuid1",
            "Player1#NA1",
            TeamId::BLUE,
            true,
            5,
            2,
            10,
            0,
            None,
            false,
        );
        let team1 = create_test_team(TeamId::BLUE, true);
        let team2 = create_test_team(TeamId::RED, false);
        let test_match = create_test_match(
            &match_id,
            queue_id,
            Some(now),
            vec![participant1],
            vec![team1, team2],
        );

        // Call the actual async update function
        update_match(
            &pool,
            &match_id,
            i64::from(u16::from(queue_id)),
            is_win,
            now,
            &test_match,
        )
        .await?;

        // Call the actual async fetch function
        let matches_to_fetch = vec![match_id.clone()];
        let seen_matches_map = fetch_seen_events(&pool, &matches_to_fetch).await?;

        assert_eq!(seen_matches_map.len(), 1);
        let fetched_info = seen_matches_map.get(&match_id).unwrap();

        assert_eq!(fetched_info.id, match_id);
        assert_eq!(
            fetched_info.queue_id,
            i64::from(u16::from(queue_id)).to_string()
        );
        assert_eq!(fetched_info.win, is_win);
        // Compare timestamps directly
        assert_eq!(fetched_info.end_timestamp.timestamp_millis(), now);
        assert!(fetched_info.match_info.is_some());
        assert_eq!(
            fetched_info.match_info.as_ref().unwrap().metadata.match_id,
            match_id
        );

        Ok(())
    }
}
