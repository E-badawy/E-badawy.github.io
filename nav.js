// Keep user-facing URLs clean (no .html) while preserving static-page routing.
(function normalizePrettyUrl() {
  const path = window.location.pathname;
  let cleanPath = null;

  if (/\/index\.html$/i.test(path)) {
    cleanPath = path.replace(/\/index\.html$/i, "/");
  } else if (/\.html$/i.test(path)) {
    cleanPath = path.replace(/\.html$/i, "");
  }

  if (cleanPath !== null) {
    if (!cleanPath.startsWith("/")) cleanPath = "/" + cleanPath;
    if (cleanPath === "") cleanPath = "/";
    window.history.replaceState({}, "", cleanPath + window.location.search + window.location.hash);
  }
})();

// Route extensionless internal links to static HTML files without showing .html in the URL.
(function routeExtensionlessLinks() {
  document.addEventListener("click", function (event) {
    const link = event.target.closest("a[href]");
    if (!link) return;

    const rawHref = (link.getAttribute("href") || "").trim();
    if (
      !rawHref ||
      rawHref === "/" ||
      rawHref.startsWith("#") ||
      rawHref.startsWith("/#") ||
      rawHref.includes("/#") ||
      rawHref.startsWith("mailto:") ||
      rawHref.startsWith("tel:") ||
      rawHref.startsWith("javascript:")
    ) {
      return;
    }

    if (/^https?:\/\//i.test(rawHref) || rawHref.includes(".") || rawHref.endsWith("/")) {
      return;
    }

    const path = rawHref.startsWith("/") ? rawHref : "/" + rawHref.replace(/^\.?\//, "");
    const htmlPath = path + ".html";

    if (link.target === "_blank") {
      event.preventDefault();
      window.open(htmlPath, "_blank", "noopener,noreferrer");
      return;
    }

    event.preventDefault();
    window.location.assign(htmlPath);
  });
})();

document.addEventListener("DOMContentLoaded", function () {
  const body = document.body;
  const navToggle = document.getElementById("nav-toggle");
  const topNav = document.getElementById("site-navigation");

  const ensureSkipLink = function () {
    if (document.querySelector(".skip-link")) return;

    const target =
      document.querySelector("main") ||
      document.querySelector("#about") ||
      document.querySelector("section[id]");

    if (!target) return;

    if (!target.id) {
      target.id = "main-content";
    }

    const skip = document.createElement("a");
    skip.className = "skip-link";
    skip.href = "#" + target.id;
    skip.textContent = "Skip to content";
    skip.addEventListener("click", function () {
      target.setAttribute("tabindex", "-1");
      target.focus({ preventScroll: false });
    });

    body.prepend(skip);
  };

  ensureSkipLink();

  const setupMobileViewToggle = function () {
    if (!navToggle) return;

    let toggleBtn = document.getElementById("mobile-view-toggle");
    if (!toggleBtn) {
      toggleBtn = document.createElement("button");
      toggleBtn.id = "mobile-view-toggle";
      toggleBtn.className = "mobile-view-toggle";
      toggleBtn.type = "button";
      toggleBtn.setAttribute("aria-label", "Toggle mobile view mode");
    }

    let controlsWrap = document.querySelector(".header-mobile-controls");
    if (!controlsWrap) {
      controlsWrap = document.createElement("div");
      controlsWrap.className = "header-mobile-controls";
      const parent = navToggle.parentElement;
      if (parent) {
        parent.insertBefore(controlsWrap, topNav || navToggle.nextSibling);
        controlsWrap.appendChild(navToggle);
      }
    }
    controlsWrap.appendChild(toggleBtn);

    const applyMode = function (mode) {
      const desktopFit = mode === "desktop";
      body.classList.toggle("mobile-desktop-fit", desktopFit);
      toggleBtn.textContent = desktopFit ? "Standard Mobile" : "Desktop View";
      toggleBtn.setAttribute("aria-pressed", String(desktopFit));
      toggleBtn.title = "Tap to switch between standard mobile and desktop-fit view";
    };

    // Default load is standard mobile mode.
    applyMode("mobile");

    toggleBtn.addEventListener("click", function () {
      const next = body.classList.contains("mobile-desktop-fit") ? "mobile" : "desktop";
      applyMode(next);
    });
  };

  setupMobileViewToggle();

  if (!navToggle || !topNav) return;

  const closeNav = function () {
    topNav.classList.remove("is-open");
    navToggle.classList.remove("is-open");
    navToggle.setAttribute("aria-expanded", "false");
  };

  navToggle.addEventListener("click", function () {
    const isOpen = topNav.classList.toggle("is-open");
    navToggle.classList.toggle("is-open", isOpen);
    navToggle.setAttribute("aria-expanded", String(isOpen));
  });

  topNav.querySelectorAll("a").forEach(function (link) {
    link.addEventListener("click", closeNav);
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") closeNav();
  });

  const navLinks = Array.from(topNav.querySelectorAll(".nav-btn[href*='#']"));
  const isHomePage = body.classList.contains("home-page");

  const sectionIdForLink = function (link) {
    const href = link.getAttribute("href") || "";
    const hash = href.includes("#") ? href.split("#")[1] : "";
    return decodeURIComponent(hash.trim());
  };

  const setActiveNav = function (activeId) {
    navLinks.forEach(function (link) {
      const id = sectionIdForLink(link);
      link.classList.toggle("is-active", Boolean(activeId) && id === activeId);
    });
  };

  const hashId = decodeURIComponent((window.location.hash || "").replace(/^#/, ""));
  if (hashId) setActiveNav(hashId);

  navLinks.forEach(function (link) {
    link.addEventListener("click", function () {
      const id = sectionIdForLink(link);
      if (id) setActiveNav(id);
    });
  });

  window.addEventListener("hashchange", function () {
    const current = decodeURIComponent((window.location.hash || "").replace(/^#/, ""));
    if (current) setActiveNav(current);
  });

  if (isHomePage) {
    const observedSections = navLinks
      .map(function (link) {
        const id = sectionIdForLink(link);
        return id ? document.getElementById(id) : null;
      })
      .filter(Boolean);

    if (observedSections.length > 0) {
      const visibilityMap = new Map();

      const observer = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            const id = entry.target.id;
            const ratio = entry.isIntersecting ? entry.intersectionRatio : 0;
            visibilityMap.set(id, ratio);
          });

          let bestId = "";
          let bestRatio = 0;
          visibilityMap.forEach(function (ratio, id) {
            if (ratio > bestRatio) {
              bestRatio = ratio;
              bestId = id;
            }
          });

          if (bestId) setActiveNav(bestId);
        },
        {
          threshold: [0.2, 0.35, 0.5, 0.7],
          rootMargin: "-18% 0px -52% 0px",
        }
      );

      observedSections.forEach(function (section) {
        observer.observe(section);
      });
    }
  }
});
