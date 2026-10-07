// kurwadb through pgjdbc, running the catalog queries DBeaver 26 sends to
// fill its navigator (schemas, tables, columns, types) as prepared statements,
// and JDBC's own DatabaseMetaData.
//
//     javac -cp postgresql.jar JdbcCheck.java   (or: java -jar ecj.jar -21 ...)
//     PGPASSWORD=... java -cp postgresql.jar:. JdbcCheck 127.0.0.1:5499
import java.sql.*;
import java.util.*;

public class JdbcCheck {
  static Connection c;
  static int checks = 0;

  static List<Map<String, String>> q(String sql, Object... params) throws Exception {
    try (PreparedStatement st = c.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        if (params[i] instanceof Long l) st.setLong(i + 1, l); else st.setString(i + 1, (String) params[i]);
      }
      try (ResultSet rs = st.executeQuery()) {
        List<Map<String, String>> out = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        while (rs.next()) {
          Map<String, String> row = new LinkedHashMap<>();
          for (int i = 1; i <= md.getColumnCount(); i++) row.put(md.getColumnLabel(i), rs.getString(i));
          out.add(row);
        }
        return out;
      }
    }
  }

  static void check(String label, boolean ok) {
    if (!ok) { System.err.println("FAIL " + label); System.exit(1); }
    checks++;
  }

  public static void main(String[] a) throws Exception {
    Properties p = new Properties();
    p.setProperty("user", "dbeaver");
    p.setProperty("password", System.getenv().getOrDefault("PGPASSWORD", "x"));
    p.setProperty("ApplicationName", "DBeaver check");
    c = DriverManager.getConnection("jdbc:postgresql://" + a[0] + "/kurwadb", p);
    c.setAutoCommit(true);

    try (Statement s = c.createStatement()) {
      s.execute("CREATE SCHEMA IF NOT EXISTS jdbcschema");
      s.execute("CREATE TABLE IF NOT EXISTS jdbcschema.things (key text)");
      s.execute("INSERT INTO jdbcschema.things VALUES ('t1') ON CONFLICT DO NOTHING");
    }

    var path = q("SELECT reset_val FROM pg_settings WHERE name = 'search_path'");
    check("search_path", path.size() == 1);
    var cur = q("SELECT current_schema(),session_user");
    check("current_schema " + cur, cur.size() == 1 && "public".equals(cur.get(0).get("current_schema")));

    var db = q("SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname=?", "kurwadb");
    check("database", db.size() == 1);

    var schemas = q("SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n");
    String nsOid = null;
    for (var r : schemas) if ("jdbcschema".equals(r.get("nspname"))) nsOid = r.get("oid");
    check("schema listed " + schemas, nsOid != null);

    var tables = q("SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \nFROM pg_catalog.pg_class c\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\nWHERE c.relnamespace=? AND c.relkind not in ('i','I','c')", Long.parseLong(nsOid));
    check("tables " + tables, tables.size() == 1 && "things".equals(tables.get(0).get("relname")));

    var cols = q("SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid\nFROM pg_catalog.pg_attribute a\nINNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\nLEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\nLEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid\nWHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.relnamespace=? ORDER BY a.attnum", Long.parseLong(nsOid));
    check("columns " + cols, cols.size() == 1 && "key".equals(cols.get(0).get("attname")));

    var type = q("SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name FROM pg_catalog.pg_type t\nLEFT OUTER JOIN pg_class c ON c.oid=t.typrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid\nWHERE t.oid=? ", 25L);
    check("type text", type.size() == 1 && "text".equals(type.get(0).get("typname")));

    var data = q("SELECT key FROM jdbcschema.things WHERE key = ?", "t1");
    check("data", data.size() == 1);

    // what DBeaver's own JDBC metadata calls send
    DatabaseMetaData md = c.getMetaData();
    int n = 0;
    try (ResultSet rs = md.getTables(null, "jdbcschema", "%", new String[] {"TABLE"})) { while (rs.next()) n++; }
    check("getTables " + n, n == 1);
    n = 0;
    try (ResultSet rs = md.getSchemas()) { while (rs.next()) n++; }
    check("getSchemas " + n, n >= 2);

    System.out.println("pgjdbc (DBeaver queries): " + checks + " checks passed");
  }
}
