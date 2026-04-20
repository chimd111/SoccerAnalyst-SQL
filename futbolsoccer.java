import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class futbolsoccer {

    private static final int PAGE_SIZE = 20;
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_RESET = "\u001B[0m";

    private static final int BATCH_SIZE = 500;
    private static final String BASE_PATH = "sql";

    private static final String[] REPOPULATE_SCRIPTS = {
            "competitions_inserts.txt", "countries.txt", "clubs_inserts.txt",
            "players.txt", "games.txt", "transfers.txt", "participate.txt",
            "club_games_1.txt", "club_games_2.txt", "club_games_3.txt",
            "player_valuation_1.txt", "player_valuation_2.txt", "player_valuation_3.txt"
    };

    public static void main(String[] args) {
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream("auth.cfg")) {
            props.load(fis);
        } catch (IOException e) {
            System.out.println("Could not read auth.cfg");
            System.out.println("Make sure auth.cfg exists in the project root.");
            return;
        }

        String host = props.getProperty("db.host");
        String instance = props.getProperty("db.instance");
        String dbName = props.getProperty("db.name");

        if (host == null || instance == null || dbName == null
                || host.trim().isEmpty() || instance.trim().isEmpty() || dbName.trim().isEmpty()) {
            System.out.println("auth.cfg is missing required values.");
            System.out.println("Required keys: db.host, db.instance, db.name");
            return;
        }

        String connectionUrl = "jdbc:sqlserver://" + host + ";"
                + "instanceName=" + instance + ";"
                + "databaseName=" + dbName + ";"
                + "integratedSecurity=true;"
                + "encrypt=false;"
                + "trustServerCertificate=true;"
                + "loginTimeout=30;";

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("SQL Server JDBC driver not found.");
            return;
        }

        try (Connection connection = DriverManager.getConnection(connectionUrl);
                Scanner scanner = new Scanner(System.in)) {
            runMainMenu(connection, scanner);
        } catch (SQLException e) {
            System.out.println("Database connection failed.");
            e.printStackTrace();
        }
    }
    // --- PRESENTATION LAYER: SCREEN MANAGEMENT ---

    private static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    private static void pauseToReturn(Scanner scanner) {
        System.out.println();
        System.out.print(ANSI_RED + "Press [Enter] to return to the menu..." + ANSI_RESET);
        scanner.nextLine();
    }

    // --- MENUS ---

    private static void runMainMenu(Connection connection, Scanner scanner) {
        boolean running = true;
        while (running) {
            clearScreen();
            System.out.println("================ SOCCER ANALYST CONSOLE ================");
            System.out.println("  1. Create Database for new Users");
            System.out.println("  2. View Raw Database Tables");
            System.out.println("  3. Standard Analyst Queries");
            System.out.println("  4. Advanced Complex Queries (Multi-Table Joins)");
            System.out.println("  5. Database Maintenance");
            System.out.println("  0. Exit Application");
            System.out.println("========================================================");
            System.out.print("Select an option: ");
            String choice = scanner.nextLine().trim();

            if (choice.equals("1"))
                createDb(connection, scanner);
            else if (choice.equals("2"))
                runTablesMenu(connection, scanner);
            else if (choice.equals("3"))
                runAnalystMenu(connection, scanner);
            else if (choice.equals("4"))
                runAdvancedMenu(connection, scanner);
            else if (choice.equals("5"))
                runMaintenanceMenu(connection, scanner);
            else if (choice.equals("0"))
                running = false;
        }
        clearScreen();
        System.out.println("Exiting Soccer Analyst Console. Goodbye!");
    }

    private static void createDb(Connection connection, Scanner scanner) {
        System.out.print("Type CREATE to initialize database: ");
        String confirmation = scanner.nextLine().trim();

        if (!"CREATE".equalsIgnoreCase(confirmation)) {
            System.out.println("Initialization cancelled.");
            pauseToReturn(scanner);
            return;
        }

        try {
            boolean tablesExist = tableExists(connection, "countries");

            if (!tablesExist) {
                System.out.println("No tables found. Creating schema...");
                runSingleSqlFile(connection, "schema.txt");
                System.out.println("Tables created.");

                System.out.println("Populating database...");
                runRepopulateScripts(connection);
                System.out.println("Database populated successfully.");
            } else {
                System.out.println("Tables already exist. Initialization skipped.");
            }

        } catch (SQLException | IOException e) {
            System.out.println("Initialization failed: " + e.getMessage());
        }

        pauseToReturn(scanner);
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    private static void runSingleSqlFile(Connection connection, String fileName) throws SQLException, IOException {
        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try (Statement stmt = connection.createStatement()) {
            String scriptPath = resolveScriptPath(fileName);
            runSqlScript(stmt, scriptPath);
            connection.commit();
        } catch (SQLException | IOException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    private static void runTablesMenu(Connection connection, Scanner scanner) {
        boolean running = true;
        while (running) {
            clearScreen();
            System.out.println("================ RAW TABLES ================");
            System.out.println("  1. View countries");
            System.out.println("  2. View competitions");
            System.out.println("  3. View clubs");
            System.out.println("  4. View games");
            System.out.println("  5. View players");
            System.out.println("  6. View participate");
            System.out.println("  7. View player_valuation");
            System.out.println("  8. View transfers");
            System.out.println("  9. View club_games");
            System.out.println("  0. Go Back");
            System.out.println("============================================");
            System.out.print("Select an option: ");
            String choice = scanner.nextLine().trim();

            boolean userAborted = false;
            try {
                clearScreen();
                if (choice.equals("1"))
                    userAborted = showTable(connection, "countries", "country_id", scanner);
                else if (choice.equals("2"))
                    userAborted = showTable(connection, "competitions", "CAST([competition_id] AS VARCHAR(255))",
                            scanner);
                else if (choice.equals("3"))
                    userAborted = showTable(connection, "clubs", "club_id", scanner);
                else if (choice.equals("4"))
                    userAborted = showTable(connection, "games", "game_id", scanner);
                else if (choice.equals("5"))
                    userAborted = showTable(connection, "players", "player_id", scanner);
                else if (choice.equals("6"))
                    userAborted = showTable(connection, "participate",
                            "player_id, CAST([competition_id] AS VARCHAR(255))", scanner);
                else if (choice.equals("7"))
                    userAborted = showTable(connection, "player_valuation", "player_valuation_id", scanner);
                else if (choice.equals("8"))
                    userAborted = showTable(connection, "transfers", "transfer_id", scanner);
                else if (choice.equals("9"))
                    userAborted = showTable(connection, "club_games", "game_id, club_id", scanner);
                else if (choice.equals("0")) {
                    running = false;
                    continue;
                } else {
                    System.out.println("Invalid option.");
                }
            } catch (SQLException e) {
                System.out.println("Query failed: " + e.getMessage());
            }

            // Only pause if the user DID NOT press 'q'
            if (!userAborted && running)
                pauseToReturn(scanner);
        }
    }

    private static void runAnalystMenu(Connection connection, Scanner scanner) {
        boolean running = true;
        while (running) {
            clearScreen();
            System.out.println("================ STANDARD ANALYST QUERIES ================");
            System.out.println(" 10. Top players by market value");
            System.out.println(" 11. Clubs by average player market value");
            System.out.println(" 12. Competitions with the most participating players");
            System.out.println(" 13. Clubs with the most wins");
            System.out.println(" 14. Clubs with the most loss");
            System.out.println(" 15. Biggest transfers");
            System.out.println(" 16. Highest-valued player in each position");
            System.out.println(" 17. Top clubs with most goals");
            System.out.println(" 18. Number of games in each competition");
            System.out.println(" 19. Players in a chosen competition");
            System.out.println(" 20. Transfer activity by club");
            System.out.println(" 21. Valuation history for a chosen player");
            System.out.println("  0. Go Back");
            System.out.println("==========================================================");
            System.out.print("Select an option: ");
            String choice = scanner.nextLine().trim();

            boolean userAborted = false;
            try {
                clearScreen();
                if (choice.equals("10"))
                    userAborted = topPlayersByMarketValue(connection, scanner);
                else if (choice.equals("11"))
                    userAborted = clubsByAveragePlayerValue(connection, scanner);
                else if (choice.equals("12"))
                    userAborted = competitionsByParticipation(connection, scanner);
                else if (choice.equals("13"))
                    userAborted = clubsByWins(connection, scanner);
                else if (choice.equals("14"))
                    userAborted = clubsWithMostLoss(connection, scanner);
                else if (choice.equals("15"))
                    userAborted = biggestTransfers(connection, scanner);
                else if (choice.equals("16"))
                    userAborted = highestValuedPlayersByPosition(connection, scanner);
                else if (choice.equals("17"))
                    userAborted = clubsWithMostGoals(connection, scanner);
                else if (choice.equals("18"))
                    userAborted = gamesInEachCompetition(connection, scanner);
                else if (choice.equals("19"))
                    userAborted = playersInCompetition(connection, scanner);
                else if (choice.equals("20"))
                    userAborted = transferActivityByClub(connection, scanner);
                else if (choice.equals("21"))
                    userAborted = valuationHistoryForPlayer(connection, scanner);
                else if (choice.equals("0")) {
                    running = false;
                    continue;
                } else {
                    System.out.println("Invalid option.");
                }
            } catch (SQLException e) {
                System.out.println("Query failed: " + e.getMessage());
            }

            if (!userAborted && running)
                pauseToReturn(scanner);
        }
    }

    private static void runAdvancedMenu(Connection connection, Scanner scanner) {
        boolean running = true;
        while (running) {
            clearScreen();
            System.out.println("================ ADVANCED COMPLEX QUERIES ================");
            System.out.println(" 24. Squad Value vs. Historical Win Percentage");
            System.out.println(" 25. Competition Financial Health (Avg Player Value & Transfer Volume)");
            System.out.println(" 26. The 'Bad Investment' Pipeline (High Transfer Spend, Most Losses)");
            System.out.println("  0. Go Back");
            System.out.println("==========================================================");
            System.out.print("Select an option: ");
            String choice = scanner.nextLine().trim();

            boolean userAborted = false;
            try {
                clearScreen();
                if (choice.equals("24"))
                    userAborted = squadValueVsWinPercentage(connection, scanner);
                else if (choice.equals("25"))
                    userAborted = competitionFinancialHealth(connection, scanner);
                else if (choice.equals("26"))
                    userAborted = highSpendersMostLosses(connection, scanner);
                else if (choice.equals("0")) {
                    running = false;
                    continue;
                } else {
                    System.out.println("Invalid option.");
                }
            } catch (SQLException e) {
                System.out.println("Query failed: " + e.getMessage());
            }

            if (!userAborted && running)
                pauseToReturn(scanner);
        }
    }

    private static void runMaintenanceMenu(Connection connection, Scanner scanner) {
        boolean running = true;
        while (running) {
            clearScreen();
            System.out.println("================ DATABASE MAINTENANCE ================");
            System.out.println(" 22. Delete all data");
            System.out.println(" 23. Repopulate database from SQL files");
            System.out.println("  0. Go Back");
            System.out.println("======================================================");
            System.out.print("Select an option: ");
            String choice = scanner.nextLine().trim();

            try {
                clearScreen();
                if (choice.equals("22"))
                    deleteAllData(connection, scanner);
                else if (choice.equals("23"))
                    repopulateDatabase(connection, scanner);
                else if (choice.equals("0")) {
                    running = false;
                    continue;
                } else
                    System.out.println("Invalid option.");
            } catch (SQLException | IOException e) {
                System.out.println("Operation failed: " + e.getMessage());
            }
            if (running)
                pauseToReturn(scanner); // No pagination here, so always pause
        }
    }

    private static String resolveScriptPath(String fileName) {
        return Paths.get(System.getProperty("user.dir"), BASE_PATH, fileName).toString();
    }

    // ==============================================================================
    // --- SQL QUERIES ---
    // ==============================================================================

    private static boolean showTable(Connection connection, String tableName, String orderBy, Scanner scanner)
            throws SQLException {
        String sql = "SELECT * FROM [" + tableName + "] ORDER BY " + orderBy + ";";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean topPlayersByMarketValue(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT [player_id], CAST([name] AS VARCHAR(255)) AS [name], CAST([position] AS VARCHAR(255)) AS [position], CAST([current_club_name] AS VARCHAR(255)) AS [current_club_name], CAST([market_value_in_eur] AS DECIMAL(18,2)) AS [market_value_in_eur] FROM [players] WHERE [market_value_in_eur] IS NOT NULL ORDER BY [market_value_in_eur] DESC, CAST([name] AS VARCHAR(255)) ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean clubsByAveragePlayerValue(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT c.[club_id], CAST(c.[name] AS VARCHAR(255)) AS [club_name], COUNT(p.[player_id]) AS [player_count], CAST(AVG(CAST(p.[market_value_in_eur] AS FLOAT)) AS DECIMAL(18,2)) AS [avg_market_value] FROM [clubs] c JOIN [players] p ON c.[club_id] = p.[current_club_id] WHERE p.[market_value_in_eur] IS NOT NULL GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [avg_market_value] DESC, [player_count] DESC, [club_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean competitionsByParticipation(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT CAST(c.[competition_id] AS VARCHAR(255)) AS [competition_id], CAST(c.[name] AS VARCHAR(255)) AS [competition_name], COUNT(*) AS [participating_players] FROM [competitions] c JOIN [participate] p ON CAST(c.[competition_id] AS VARCHAR(255)) = CAST(p.[competition_id] AS VARCHAR(255)) GROUP BY CAST(c.[competition_id] AS VARCHAR(255)), CAST(c.[name] AS VARCHAR(255)) ORDER BY [participating_players] DESC, [competition_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean clubsByWins(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT c.[club_id], CAST(c.[name] AS VARCHAR(255)) AS [club_name], COUNT(*) AS [wins] FROM [clubs] c JOIN [club_games] cg ON c.[club_id] = cg.[club_id] WHERE cg.[is_win] = 1 GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [wins] DESC, [club_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean clubsWithMostLoss(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT c.[club_id], CAST(c.[name] AS VARCHAR(255)) AS [club_name], COUNT(*) AS [losses] FROM [clubs] c JOIN [club_games] cg ON c.[club_id] = cg.[club_id] WHERE cg.[is_win] = 0 GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [losses] DESC, [club_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean gamesInEachCompetition(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT CAST(c.[competition_id] AS VARCHAR(255)) AS [competition_id], CAST(c.[name] AS VARCHAR(255)) AS [competition_name], COUNT(g.[game_id]) AS [num_games] FROM [competitions] c LEFT JOIN [games] g ON c.[competition_id] = g.[competition_id] GROUP BY CAST(c.[competition_id] AS VARCHAR(255)), CAST(c.[name] AS VARCHAR(255)) ORDER BY [num_games] DESC, [competition_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean biggestTransfers(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT t.[transfer_id], CAST(p.[name] AS VARCHAR(255)) AS [player_name], CAST(fc.[name] AS VARCHAR(255)) AS [from_club], CAST(tc.[name] AS VARCHAR(255)) AS [to_club], CAST(t.[transfer_date] AS VARCHAR(255)) AS [transfer_date], CAST(t.[transfer_fee] AS DECIMAL(18,2)) AS [transfer_fee], CAST(t.[market_value_in_eur] AS DECIMAL(18,2)) AS [market_value_in_eur] FROM [transfers] t LEFT JOIN [players] p ON t.[player_id] = p.[player_id] LEFT JOIN [clubs] fc ON t.[from_club_id] = fc.[club_id] LEFT JOIN [clubs] tc ON t.[to_club_id] = tc.[club_id] WHERE t.[transfer_fee] IS NOT NULL ORDER BY t.[transfer_fee] DESC, CAST(t.[transfer_date] AS VARCHAR(255)) DESC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean highestValuedPlayersByPosition(Connection connection, Scanner scanner) throws SQLException {
        String sql = "WITH ranked_players AS (SELECT [player_id], CAST([name] AS VARCHAR(255)) AS [player_name], CAST([position] AS VARCHAR(255)) AS [player_position], CAST([current_club_name] AS VARCHAR(255)) AS [club_name], [market_value_in_eur], ROW_NUMBER() OVER (PARTITION BY CAST([position] AS VARCHAR(255)) ORDER BY [market_value_in_eur] DESC, CAST([name] AS VARCHAR(255)) ASC) AS [rn] FROM [players] WHERE [position] IS NOT NULL AND [market_value_in_eur] IS NOT NULL) SELECT [player_position], [player_id], [player_name], [club_name], CAST([market_value_in_eur] AS DECIMAL(18,2)) AS [market_value_in_eur] FROM ranked_players WHERE [rn] = 1 ORDER BY [player_position] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean clubsWithMostGoals(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT c.[club_id], CAST(c.[name] AS VARCHAR(255)) AS [club_name], SUM(cg.[own_goals]) AS [total_goals] FROM [clubs] c JOIN [club_games] cg ON c.[club_id] = cg.[club_id] WHERE cg.[own_goals] IS NOT NULL GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [total_goals] DESC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean playersInCompetition(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter competition_id exactly as stored (example: GB1): ");
        String competitionId = scanner.nextLine().trim();
        if (competitionId.isEmpty())
            return false;
        String sql = "SELECT p.[player_id], CAST(p.[name] AS VARCHAR(255)) AS [name], CAST(p.[position] AS VARCHAR(255)) AS [position], CAST(p.[current_club_name] AS VARCHAR(255)) AS [current_club_name] FROM [players] p JOIN [participate] pa ON p.[player_id] = pa.[player_id] WHERE CAST(pa.[competition_id] AS VARCHAR(255)) = ? ORDER BY CAST(p.[current_club_name] AS VARCHAR(255)) ASC, CAST(p.[name] AS VARCHAR(255)) ASC;";
        List<Object> params = new ArrayList<>();
        params.add(competitionId);
        return executeAndPrint(connection, sql, params, scanner);
    }

    private static boolean transferActivityByClub(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT c.[club_id], CAST(c.[name] AS VARCHAR(255)) AS [club_name], COUNT(*) AS [transfer_events] FROM [clubs] c JOIN [transfers] t ON c.[club_id] = t.[to_club_id] OR c.[club_id] = t.[from_club_id] GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [transfer_events] DESC, [club_name] ASC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean valuationHistoryForPlayer(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter player_id: ");
        String input = scanner.nextLine().trim();
        if (input.isEmpty()) {
            System.out.println("player_id cannot be empty.");
            return false;
        }
        int playerId;
        try {
            playerId = Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.out.println("Please enter a valid numeric player_id.");
            return false;
        }
        String sql = "SELECT pv.[player_valuation_id], CAST(p.[name] AS VARCHAR(255)) AS [player_name], CAST(pv.[date] AS VARCHAR(255)) AS [date], CAST(pv.[market_value_in_eur] AS DECIMAL(18,2)) AS [market_value_in_eur], CAST(pv.[player_club_domestic_competition_id] AS VARCHAR(255)) AS [player_club_domestic_competition_id] FROM [player_valuation] pv LEFT JOIN [players] p ON pv.[player_id] = p.[player_id] WHERE pv.[player_id] = ? ORDER BY CAST(pv.[date] AS VARCHAR(255)) DESC, pv.[player_valuation_id] DESC;";
        List<Object> params = new ArrayList<>();
        params.add(playerId);
        return executeAndPrint(connection, sql, params, scanner);
    }

    private static boolean squadValueVsWinPercentage(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT CAST(c.[name] AS VARCHAR(255)) AS [club_name], CAST(SUM(CAST(p.[market_value_in_eur] AS FLOAT)) AS DECIMAL(18,2)) AS [squad_market_value], CAST((SUM(CASE WHEN cg.[is_win] = 1 THEN 1.0 ELSE 0.0 END) / COUNT(cg.[game_id])) * 100 AS DECIMAL(5,2)) AS [win_percentage] FROM [clubs] c JOIN [players] p ON c.[club_id] = p.[current_club_id] JOIN [club_games] cg ON c.[club_id] = cg.[club_id] GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [squad_market_value] DESC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean competitionFinancialHealth(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT CAST(comp.[name] AS VARCHAR(255)) AS [competition_name], COUNT(DISTINCT p.[player_id]) AS [total_players], CAST(AVG(CAST(p.[market_value_in_eur] AS FLOAT)) AS DECIMAL(18,2)) AS [avg_player_value], CAST(SUM(CAST(t.[transfer_fee] AS FLOAT)) AS DECIMAL(18,2)) AS [total_transfer_spending] FROM [competitions] comp JOIN [participate] pa ON CAST(comp.[competition_id] AS VARCHAR(255)) = CAST(pa.[competition_id] AS VARCHAR(255)) JOIN [players] p ON pa.[player_id] = p.[player_id] LEFT JOIN [transfers] t ON p.[player_id] = t.[player_id] GROUP BY CAST(comp.[competition_id] AS VARCHAR(255)), CAST(comp.[name] AS VARCHAR(255)) ORDER BY [avg_player_value] DESC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    private static boolean highSpendersMostLosses(Connection connection, Scanner scanner) throws SQLException {
        String sql = "SELECT CAST(c.[name] AS VARCHAR(255)) AS [club_name], CAST(SUM(CAST(t.[transfer_fee] AS FLOAT)) AS DECIMAL(18,2)) AS [total_money_spent], SUM(CASE WHEN cg.[is_win] = 0 THEN 1 ELSE 0 END) AS [total_losses] FROM [clubs] c JOIN [transfers] t ON c.[club_id] = t.[to_club_id] JOIN [club_games] cg ON c.[club_id] = cg.[club_id] WHERE t.[transfer_fee] IS NOT NULL GROUP BY c.[club_id], CAST(c.[name] AS VARCHAR(255)) ORDER BY [total_losses] DESC, [total_money_spent] DESC;";
        return executeAndPrint(connection, sql, new ArrayList<>(), scanner);
    }

    // --- DATABASE MAINTENANCE ---

    private static void deleteAllData(Connection connection, Scanner scanner) throws SQLException {
        // Code unchanged
        System.out.print("Type DELETE to confirm removing all rows from all tables: ");
        String confirmation = scanner.nextLine().trim();
        if (!"DELETE".equals(confirmation)) {
            System.out.println("Delete cancelled.");
            return;
        }
        String[] deleteStatements = { "DELETE FROM [club_games];", "DELETE FROM [transfers];",
                "DELETE FROM [player_valuation];", "DELETE FROM [participate];", "DELETE FROM [players];",
                "DELETE FROM [games];", "DELETE FROM [clubs];", "DELETE FROM [competitions];",
                "DELETE FROM [countries];" };
        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement stmt = connection.createStatement()) {
            for (String s : deleteStatements)
                stmt.executeUpdate(s);
            connection.commit();
            System.out.println("All table data deleted.");
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    private static void repopulateDatabase(Connection connection, Scanner scanner) throws SQLException, IOException {
        System.out.print("Type REPOPULATE to confirm running local SQL files: ");
        String confirmation = scanner.nextLine().trim();
        if (!"REPOPULATE".equals(confirmation)) {
            System.out.println("Repopulate cancelled.");
            return;
        }
        runRepopulateScripts(connection);
    }

    private static void runRepopulateScripts(Connection connection) throws SQLException, IOException {

        boolean originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < REPOPULATE_SCRIPTS.length; i++) {
                String scriptPath = resolveScriptPath(REPOPULATE_SCRIPTS[i]);
                System.out.println("Loading file " + (i + 1) + " of " + REPOPULATE_SCRIPTS.length + ": " + scriptPath);
                runSqlScript(stmt, scriptPath);
                connection.commit();
            }
            System.out.println("Repopulation completed.");
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    private static void runSqlScript(Statement stmt, String fileName) throws IOException, SQLException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName), 1 << 16)) {
            StringBuilder command = new StringBuilder();
            String line;
            int totalStatements = 0, batchCount = 0;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("--"))
                    continue;
                if (trimmed.equalsIgnoreCase("GO")) {
                    if (addBufferedSqlToBatch(stmt, command)) {
                        totalStatements++;
                        batchCount++;
                    }
                    if (batchCount >= BATCH_SIZE) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                        batchCount = 0;
                    }
                    continue;
                }
                command.append(line).append(' ');
                if (trimmed.endsWith(";")) {
                    if (addBufferedSqlToBatch(stmt, command)) {
                        totalStatements++;
                        batchCount++;
                    }
                    if (batchCount >= BATCH_SIZE) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                        batchCount = 0;
                    }
                }
            }
            if (addBufferedSqlToBatch(stmt, command)) {
                totalStatements++;
                batchCount++;
            }
            if (batchCount > 0) {
                stmt.executeBatch();
                stmt.clearBatch();
            }
            System.out.println("Finished " + fileName + " with " + totalStatements + " statements executed.");
        }
    }

    private static boolean addBufferedSqlToBatch(Statement stmt, StringBuilder command) throws SQLException {
        String sql = command.toString().trim();
        if (sql.isEmpty())
            return false;
        if (sql.endsWith(";"))
            sql = sql.substring(0, sql.length() - 1).trim();
        if (sql.isEmpty()) {
            command.setLength(0);
            return false;
        }
        stmt.addBatch(sql);
        command.setLength(0);
        return true;
    }

    // ==============================================================================
    // --- FORMATTING & PRINTING METHODS ---
    // ==============================================================================

    private static boolean executeAndPrint(Connection connection, String sql, List<Object> params, Scanner scanner)
            throws SQLException {
        System.out.println("Running query...");
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                Object value = params.get(i);
                if (value instanceof Integer) {
                    ps.setInt(i + 1, (Integer) value);
                } else {
                    ps.setString(i + 1, String.valueOf(value));
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                return printResultSet(rs, scanner);
            }
        }
    }

    private static String formatHeaderName(String header) {
        if (header == null)
            return "NULL";
        String[] words = header.split("_");
        StringBuilder formatted = new StringBuilder();
        for (String word : words) {
            if (word.length() > 0) {
                formatted.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1).toLowerCase())
                        .append(" ");
            }
        }
        return formatted.toString().trim();
    }

    private static String formatNumberIfApplicable(String value, String rawColumnName) {
        String lowerCol = rawColumnName.toLowerCase();
        if (lowerCol.endsWith("id") || lowerCol.contains("date") || lowerCol.contains("year"))
            return value;
        if (value.matches("-?\\d+(\\.\\d+)?")) {
            try {
                double num = Double.parseDouble(value);
                if (Math.abs(num) >= 1000) {
                    if (value.contains("."))
                        return String.format("%,.2f", num);
                    else
                        return String.format("%,d", (long) num);
                }
            } catch (Exception e) {
                return value;
            }
        }
        return value;
    }

    // RETURNS TRUE if user typed 'q' to abort, FALSE if they naturally reached the
    // end.
    private static boolean printResultSet(ResultSet rs, Scanner scanner) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        String[] rawHeaders = new String[columnCount];
        String[] cleanHeaders = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            rawHeaders[i] = meta.getColumnLabel(i + 1);
            cleanHeaders[i] = formatHeaderName(rawHeaders[i]);
        }

        boolean hasMoreData = true;
        int totalShown = 0;

        while (hasMoreData) {
            List<String[]> rows = new ArrayList<>();
            int countThisPage = 0;
            int[] widths = new int[columnCount];

            for (int i = 0; i < columnCount; i++) {
                widths[i] = Math.max(cleanHeaders[i].length(), 12);
            }

            while (countThisPage < PAGE_SIZE && rs.next()) {
                String[] row = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    String value = rs.getString(i + 1);
                    if (value == null) {
                        value = "NULL";
                    } else {
                        value = formatNumberIfApplicable(value, rawHeaders[i]);
                    }
                    if (value.length() > 40) {
                        value = value.substring(0, 37) + "...";
                    }
                    row[i] = value;
                    widths[i] = Math.max(widths[i], value.length());
                }
                rows.add(row);
                countThisPage++;
                totalShown++;
            }

            if (totalShown == 0) {
                System.out.println("No rows returned.");
                return false;
            }

            if (countThisPage > 0) {
                System.out.println();
                printRow(cleanHeaders, widths);
                printDivider(widths);
                for (String[] row : rows) {
                    printRow(row, widths);
                }
                System.out.println("\nShowing rows " + (totalShown - countThisPage + 1) + " to " + totalShown + ".");
            }

            // --- STRICT PAGINATION INPUT LOOP (With direct exit capability) ---
            if (countThisPage == PAGE_SIZE) {
                while (true) {
                    System.out
                            .print(ANSI_RED + "Press [Enter] to view next 20 rows, or type 'q' to stop: " + ANSI_RESET);
                    String input = scanner.nextLine().trim();

                    if (input.equalsIgnoreCase("q")) {
                        // Immediately return true so the menu knows we aborted and shouldn't pause!
                        return true;
                    } else if (input.isEmpty()) {
                        break;
                    } else {
                        System.out.println("Invalid input. Please press exactly [Enter] or 'q'.");
                    }
                }
            } else {
                System.out.println("End of results.");
                hasMoreData = false;
            }
        }
        return false;
    }

    private static void printRow(String[] values, int[] widths) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            line.append(padRight(values[i], widths[i]));
            if (i < values.length - 1) {
                line.append(" | ");
            }
        }
        System.out.println(line.toString());
    }

    private static void printDivider(int[] widths) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            for (int j = 0; j < widths[i]; j++) {
                line.append('-');
            }
            if (i < widths.length - 1) {
                line.append("-+-");
            }
        }
        System.out.println(line.toString());
    }

    private static String padRight(String value, int width) {
        StringBuilder builder = new StringBuilder(value);
        while (builder.length() < width) {
            builder.append(' ');
        }
        return builder.toString();
    }
}