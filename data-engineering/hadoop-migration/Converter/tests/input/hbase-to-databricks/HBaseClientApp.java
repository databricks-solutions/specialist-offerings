package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sample HBase client application performing CRUD operations
 * on the customers and transactions tables.
 */
public class HBaseClientApp {

    private static final String TABLE_CUSTOMERS = "customers";
    private static final String TABLE_TRANSACTIONS = "transactions";
    private static final byte[] CF_INFO = Bytes.toBytes("info");
    private static final byte[] CF_ADDRESS = Bytes.toBytes("address");
    private static final byte[] CF_ORDERS = Bytes.toBytes("orders");
    private static final byte[] CF_TXN = Bytes.toBytes("txn");
    private static final byte[] CF_METADATA = Bytes.toBytes("metadata");

    private Connection connection;

    public HBaseClientApp() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zk1.example.com,zk2.example.com,zk3.example.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        this.connection = ConnectionFactory.createConnection(config);
    }

    /**
     * Insert or update a customer record.
     */
    public void upsertCustomer(String customerId, String name, String email,
                                String street, String city, String state) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_CUSTOMERS))) {
            Put put = new Put(Bytes.toBytes(customerId));
            put.addColumn(CF_INFO, Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(CF_INFO, Bytes.toBytes("email"), Bytes.toBytes(email));
            put.addColumn(CF_ADDRESS, Bytes.toBytes("street"), Bytes.toBytes(street));
            put.addColumn(CF_ADDRESS, Bytes.toBytes("city"), Bytes.toBytes(city));
            put.addColumn(CF_ADDRESS, Bytes.toBytes("state"), Bytes.toBytes(state));
            table.put(put);
        }
    }

    /**
     * Get a customer by row key.
     */
    public Result getCustomer(String customerId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_CUSTOMERS))) {
            Get get = new Get(Bytes.toBytes(customerId));
            get.addFamily(CF_INFO);
            get.addFamily(CF_ADDRESS);
            return table.get(get);
        }
    }

    /**
     * Scan customers by state using a SingleColumnValueFilter.
     */
    public List<Result> getCustomersByState(String state) throws IOException {
        List<Result> results = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_CUSTOMERS))) {
            Scan scan = new Scan();
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF_ADDRESS,
                Bytes.toBytes("state"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(state)
            );
            filter.setFilterIfMissing(true);
            scan.setFilter(filter);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    results.add(result);
                }
            }
        }
        return results;
    }

    /**
     * Batch insert transactions.
     */
    public void batchInsertTransactions(List<String[]> transactions) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_TRANSACTIONS))) {
            List<Put> puts = new ArrayList<>();
            for (String[] txn : transactions) {
                String rowKey = txn[0]; // txn_id
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(CF_TXN, Bytes.toBytes("customer_id"), Bytes.toBytes(txn[1]));
                put.addColumn(CF_TXN, Bytes.toBytes("amount"), Bytes.toBytes(txn[2]));
                put.addColumn(CF_TXN, Bytes.toBytes("timestamp"), Bytes.toBytes(txn[3]));
                put.addColumn(CF_METADATA, Bytes.toBytes("source"), Bytes.toBytes(txn[4]));
                puts.add(put);
            }
            table.put(puts);
        }
    }

    /**
     * Scan transactions within a row key range.
     */
    public List<Result> scanTransactionRange(String startKey, String endKey) throws IOException {
        List<Result> results = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_TRANSACTIONS))) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startKey));
            scan.withStopRow(Bytes.toBytes(endKey));
            scan.addFamily(CF_TXN);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    results.add(result);
                }
            }
        }
        return results;
    }

    /**
     * Delete a customer record.
     */
    public void deleteCustomer(String customerId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_CUSTOMERS))) {
            Delete delete = new Delete(Bytes.toBytes(customerId));
            table.delete(delete);
        }
    }

    /**
     * Increment a counter (e.g., order count).
     */
    public long incrementOrderCount(String customerId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_CUSTOMERS))) {
            return table.incrementColumnValue(
                Bytes.toBytes(customerId),
                CF_INFO,
                Bytes.toBytes("order_count"),
                1L
            );
        }
    }

    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        HBaseClientApp app = new HBaseClientApp();
        try {
            // Insert a customer
            app.upsertCustomer("cust100", "Alice Smith", "alice@example.com",
                "456 Oak Ave", "Chicago", "IL");

            // Read back
            Result customer = app.getCustomer("cust100");
            System.out.println("Customer: " + customer);

            // Scan by state
            List<Result> ilCustomers = app.getCustomersByState("IL");
            System.out.println("IL customers: " + ilCustomers.size());

        } finally {
            app.close();
        }
    }
}
