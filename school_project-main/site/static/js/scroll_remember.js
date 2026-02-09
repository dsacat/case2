const STORAGE_KEY = 'scrollPosition';
window.addEventListener('scroll', () => {
    localStorage.setItem(STORAGE_KEY, window.scrollY);
});
window.addEventListener('load', () => {
    const savedPosition = localStorage.getItem(STORAGE_KEY);
    if (savedPosition) {
        window.scrollTo({
            top: parseInt(savedPosition),
            behavior: 'instant'
        });
    }
});