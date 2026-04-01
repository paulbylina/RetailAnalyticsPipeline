import { useEffect, useState } from "react";

function App() {
  const [kpis, setKpis] = useState(null);
  const [regions, setRegions] = useState([]);
  const [error, setError] = useState("");

  useEffect(() => {
    Promise.all([
      fetch("http://127.0.0.1:8000/kpis").then((response) => {
        if (!response.ok) throw new Error("Failed to fetch KPI data");
        return response.json();
      }),
      fetch("http://127.0.0.1:8000/revenue-by-region").then((response) => {
        if (!response.ok) throw new Error("Failed to fetch region data");
        return response.json();
      }),
    ])
      .then(([kpiData, regionData]) => {
        setKpis(kpiData);
        setRegions(regionData);
      })
      .catch((err) => setError(err.message));
  }, []);

  return (
    <div style={{ padding: "2rem", fontFamily: "Arial, sans-serif" }}>
      <h1>Retail Analytics Dashboard</h1>

      {error && <p style={{ color: "red" }}>{error}</p>}

      {!kpis && !error && <p>Loading dashboard data...</p>}

      {kpis && (
        <>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
              gap: "1rem",
              marginTop: "1.5rem",
            }}
          >
            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Total Orders</h3>
              <p>{kpis.total_orders}</p>
            </div>

            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Total Revenue</h3>
              <p>${kpis.total_revenue}</p>
            </div>

            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Average Order Value</h3>
              <p>${kpis.avg_order_value}</p>
            </div>

            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Completed Revenue</h3>
              <p>${kpis.completed_revenue}</p>
            </div>

            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Cancelled Orders</h3>
              <p>{kpis.cancelled_orders}</p>
            </div>

            <div style={{ padding: "1rem", border: "1px solid #ddd", borderRadius: "12px" }}>
              <h3>Returned Orders</h3>
              <p>{kpis.returned_orders}</p>
            </div>
          </div>

          <h2 style={{ marginTop: "2rem" }}>Revenue by Region</h2>

          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              marginTop: "1rem",
            }}
          >
            <thead>
              <tr>
                <th style={{ textAlign: "left", borderBottom: "1px solid #ddd", padding: "0.75rem" }}>
                  Region
                </th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #ddd", padding: "0.75rem" }}>
                  Total Orders
                </th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #ddd", padding: "0.75rem" }}>
                  Total Revenue
                </th>
                <th style={{ textAlign: "left", borderBottom: "1px solid #ddd", padding: "0.75rem" }}>
                  Avg Order Value
                </th>
              </tr>
            </thead>
            <tbody>
              {regions.map((region) => (
                <tr key={region.region}>
                  <td style={{ borderBottom: "1px solid #eee", padding: "0.75rem" }}>{region.region}</td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "0.75rem" }}>{region.total_orders}</td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "0.75rem" }}>
                    ${region.total_revenue}
                  </td>
                  <td style={{ borderBottom: "1px solid #eee", padding: "0.75rem" }}>
                    ${region.avg_order_value}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export default App;