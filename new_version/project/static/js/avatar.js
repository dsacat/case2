document.addEventListener('DOMContentLoaded', () => {
    const toggle = document.getElementById('theme-toggle');
    const body = document.body;

    const applyThemeIcons = () => {
        const isDark = body.classList.contains('dark-theme');
        const items = document.querySelectorAll('[data-theme-icon]');
        items.forEach((img) => {
            const light = img.getAttribute('data-light');
            const dark = img.getAttribute('data-dark');
            if (!light || !dark) return;
            const target = isDark ? dark : light;
            const current = img.getAttribute('src');
            if (current === target) return;
            img.classList.add('is-swapping');
            setTimeout(() => img.setAttribute('src', target), 260);
            setTimeout(() => img.classList.remove('is-swapping'), 1200);
        });
    };

    applyThemeIcons();
    if (toggle) {
        toggle.addEventListener('change', () => setTimeout(applyThemeIcons, 40));
    }
    const observer = new MutationObserver(() => applyThemeIcons());
    observer.observe(body, { attributes: true, attributeFilter: ['class'] });
});
