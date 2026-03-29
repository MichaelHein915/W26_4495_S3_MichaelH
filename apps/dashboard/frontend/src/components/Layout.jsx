import { NavLink } from "react-router-dom";

export function Layout({ children }) {
  return (
    <div className="app-shell">
      <nav className="top-nav">
        <span className="top-nav__brand">⬡ CryptoAnalytics</span>
        <div className="top-nav__links">
          <NavLink to="/" end className={({ isActive }) => isActive ? "nav-link nav-link--active" : "nav-link"}>
            Market
          </NavLink>
        </div>
      </nav>
      {children}
    </div>
  );
}
