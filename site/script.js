const header = document.querySelector("[data-header]");
const navLinks = Array.from(document.querySelectorAll(".nav-links a[href^='#']"));
const sections = navLinks
  .map((link) => document.querySelector(link.getAttribute("href")))
  .filter(Boolean);

const setHeaderState = () => {
  if (!header) {
    return;
  }

  header.classList.toggle("is-scrolled", window.scrollY > 24);
};

const updateActiveLink = () => {
  let current = null;

  sections.forEach((section) => {
    if (section.offsetTop <= window.scrollY + 160) {
      current = section;
    }
  });

  navLinks.forEach((link) => {
    link.classList.toggle("is-active", current && link.getAttribute("href") === `#${current.id}`);
  });
};

window.addEventListener("scroll", () => {
  setHeaderState();
  updateActiveLink();
});

setHeaderState();
updateActiveLink();
