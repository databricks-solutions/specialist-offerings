/**
 * Java Application: Query Lakebase via JDBC
 * ==========================================
 *
 * WHAT THIS DOES:
 * - Connects Java applications to Databricks Online Tables
 * - Provides fast point lookups for enterprise Java backends
 * - Uses standard JDBC driver for compatibility
 *
 * REPLACES IN INFORMATICA:
 * - JDBC connections to Oracle/SQL Server application databases
 * - Custom sync jobs maintaining operational data stores
 *
 * USE CASES:
 * - Spring Boot microservices needing customer data
 * - Java enterprise applications (EJB, etc.)
 * - Android backend services
 * - Legacy Java systems migration
 *
 * JDBC DRIVER:
 * - Download from: https://www.databricks.com/spark/jdbc-drivers-download
 * - Maven: com.databricks:databricks-jdbc
 *
 * CONNECTION STRING FORMAT:
 * jdbc:databricks://<host>:443/default;
 *   transportMode=http;ssl=1;
 *   httpPath=/sql/1.0/warehouses/<warehouse-id>;
 *   AuthMech=3;UID=token;PWD=<access-token>
 */

import java.sql.*;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;

public class LakebaseClient {

    private final String jdbcUrl;
    private Connection connection;

    /**
     * Initialize client with Databricks connection details.
     *
     * @param host      Databricks workspace hostname (e.g., adb-xxx.azuredatabricks.net)
     * @param httpPath  SQL Warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123)
     * @param token     Personal access token or service principal token
     */
    public LakebaseClient(String host, String httpPath, String token) {
        this.jdbcUrl = String.format(
            "jdbc:databricks://%s:443/default;" +
            "transportMode=http;ssl=1;" +
            "httpPath=%s;" +
            "AuthMech=3;UID=token;PWD=%s",
            host, httpPath, token
        );
    }

    /**
     * Get or create database connection.
     * Reuses connection for better performance.
     */
    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl);
        }
        return connection;
    }

    /**
     * Lookup customer profile by ID.
     * Expected latency: < 10ms for point lookup.
     *
     * @param customerId Customer ID to lookup
     * @return Optional containing customer data if found
     */
    public Optional<CustomerProfile> getCustomerProfile(long customerId) throws SQLException {
        String query = """
            SELECT customer_id, customer_name, customer_tier,
                   lifetime_value, credit_limit, is_active
            FROM main.online.customer_profile
            WHERE customer_id = ?
            """;

        try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
            stmt.setLong(1, customerId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new CustomerProfile(
                        rs.getLong("customer_id"),
                        rs.getString("customer_name"),
                        rs.getString("customer_tier"),
                        rs.getDouble("lifetime_value"),
                        rs.getDouble("credit_limit"),
                        rs.getBoolean("is_active")
                    ));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Check inventory availability for a product.
     *
     * @param productId   Product ID
     * @param warehouseId Warehouse ID
     * @return Available quantity (0 if not found)
     */
    public int checkInventory(long productId, long warehouseId) throws SQLException {
        String query = """
            SELECT available_quantity
            FROM main.online.inventory_availability
            WHERE product_id = ? AND warehouse_id = ?
            """;

        try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
            stmt.setLong(1, productId);
            stmt.setLong(2, warehouseId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("available_quantity");
                }
            }
        }
        return 0;
    }

    /**
     * Batch lookup for multiple customers.
     * More efficient than individual lookups for multiple IDs.
     *
     * @param customerIds List of customer IDs to lookup
     * @return List of found customer profiles
     */
    public List<CustomerProfile> batchGetCustomers(List<Long> customerIds) throws SQLException {
        if (customerIds.isEmpty()) {
            return new ArrayList<>();
        }

        // Build IN clause with placeholders
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < customerIds.size(); i++) {
            placeholders.append(i > 0 ? ",?" : "?");
        }

        String query = String.format("""
            SELECT customer_id, customer_name, customer_tier,
                   lifetime_value, credit_limit, is_active
            FROM main.online.customer_profile
            WHERE customer_id IN (%s)
            """, placeholders);

        List<CustomerProfile> results = new ArrayList<>();

        try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
            for (int i = 0; i < customerIds.size(); i++) {
                stmt.setLong(i + 1, customerIds.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new CustomerProfile(
                        rs.getLong("customer_id"),
                        rs.getString("customer_name"),
                        rs.getString("customer_tier"),
                        rs.getDouble("lifetime_value"),
                        rs.getDouble("credit_limit"),
                        rs.getBoolean("is_active")
                    ));
                }
            }
        }
        return results;
    }

    /**
     * Close the database connection.
     * Call when done with the client.
     */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // Log error
        }
    }

    // ==========================================================================
    // Data classes
    // ==========================================================================

    public record CustomerProfile(
        long customerId,
        String customerName,
        String customerTier,
        double lifetimeValue,
        double creditLimit,
        boolean isActive
    ) {}

    // ==========================================================================
    // Example usage
    // ==========================================================================

    public static void main(String[] args) {
        String host = System.getenv("DATABRICKS_HOST");
        String httpPath = System.getenv("DATABRICKS_HTTP_PATH");
        String token = System.getenv("DATABRICKS_TOKEN");

        LakebaseClient client = new LakebaseClient(host, httpPath, token);

        try {
            // Single lookup
            long start = System.currentTimeMillis();
            Optional<CustomerProfile> customer = client.getCustomerProfile(12345L);
            long latency = System.currentTimeMillis() - start;

            customer.ifPresent(c -> {
                System.out.println("Customer: " + c.customerName());
                System.out.println("Tier: " + c.customerTier());
                System.out.println("Lifetime Value: $" + c.lifetimeValue());
            });
            System.out.println("Latency: " + latency + "ms");

            // Batch lookup
            List<CustomerProfile> customers = client.batchGetCustomers(
                List.of(12345L, 12346L, 12347L)
            );
            System.out.println("Found " + customers.size() + " customers");

            // Inventory check
            int qty = client.checkInventory(1001L, 5L);
            System.out.println("Available quantity: " + qty);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
