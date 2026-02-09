document.addEventListener('DOMContentLoaded', () => {
  const profileButton = document.getElementById('UserMenuButton');
  const dropdown = document.getElementById('UserMenuDropdown');
  if (!profileButton || !dropdown) return;

  const toggleMenu = (show) => {
    if (show === undefined) {
      show = !dropdown.classList.contains('show');
    }
    if (show) {
      dropdown.classList.add('show');
      profileButton.setAttribute('aria-expanded', 'true');
    } else {
      dropdown.classList.remove('show');
      profileButton.setAttribute('aria-expanded', 'false');
    }
  };

  profileButton.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });

  dropdown.addEventListener('click', (e) => e.stopPropagation());

  document.addEventListener('click', () => toggleMenu(false));

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && dropdown.classList.contains('show')) {
      toggleMenu(false);
      profileButton.focus();
    }
  });

  dropdown.querySelectorAll('[role="menuitem"]').forEach((item) => {
    item.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        item.click();
      }
    });
  });
});
