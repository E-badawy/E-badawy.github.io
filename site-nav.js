(() => {
  const navToggle = document.getElementById("nav-toggle");
  const topNav = document.getElementById("site-navigation");

  if (!navToggle || !topNav) {
    return;
  }

  navToggle.addEventListener("click", () => {
    const isOpen = topNav.classList.toggle("is-open");
    navToggle.classList.toggle("is-open", isOpen);
    navToggle.setAttribute("aria-expanded", String(isOpen));
  });

  topNav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => {
      topNav.classList.remove("is-open");
      navToggle.classList.remove("is-open");
      navToggle.setAttribute("aria-expanded", "false");
    });
  });
})();
