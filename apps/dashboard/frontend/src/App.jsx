import { Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import MarketOverview from "./pages/MarketOverview";
import SymbolDetail from "./pages/SymbolDetail";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<MarketOverview />} />
        <Route path="/symbol/:symbolId" element={<SymbolDetail />} />
        <Route path="*" element={<MarketOverview />} />
      </Routes>
    </Layout>
  );
}
