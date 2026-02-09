document.addEventListener('DOMContentLoaded', () => {
    const toggle = document.getElementById('theme-toggle');
    const slider = document.querySelector('.theme-switch .slider');
    const body = document.body;
    const eventClasses = ['event-comet', 'event-meteor', 'event-aurora', 'event-solar', 'event-lunar'];
    let eventTimer = null;

    const setColorScheme = (isDark) => {
        document.documentElement.style.colorScheme = isDark ? 'dark' : 'light';
    };

    const applyThemeState = (isDark) => {
        body.classList.toggle('dark-theme', isDark);
        setColorScheme(isDark);
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
        if (toggle) {
            toggle.checked = isDark;
        }
    };

    const saved = localStorage.getItem('theme') || 'dark';
    applyThemeState(saved === 'dark');

    const clearEventClasses = () => {
        if (!slider) return;
        eventClasses.forEach((cls) => slider.classList.remove(cls));
    };

    const runRareEvent = (forced = false) => {
        if (!slider) return;
        if (document.hidden && !forced) return;
        clearEventClasses();
        const isDark = body.classList.contains('dark-theme');
        const pool = isDark ? ['event-comet', 'event-meteor', 'event-aurora', 'event-lunar'] : ['event-solar', 'event-comet'];
        const chance = forced ? 1 : 0.28;
        if (Math.random() > chance) return;
        const selected = pool[Math.floor(Math.random() * pool.length)];
        slider.classList.add(selected);
        const duration = selected === 'event-aurora' ? 5200 : selected === 'event-lunar' ? 4200 : 3000;
        setTimeout(() => slider.classList.remove(selected), duration);
    };

    const scheduleRareEvents = () => {
        if (eventTimer) {
            clearTimeout(eventTimer);
        }
        const delay = 38000 + Math.floor(Math.random() * 92000);
        eventTimer = setTimeout(() => {
            runRareEvent(false);
            scheduleRareEvents();
        }, delay);
    };

    scheduleRareEvents();

    document.addEventListener('visibilitychange', () => {
        if (!document.hidden) {
            scheduleRareEvents();
        }
    });

    if (!toggle) return;
    toggle.addEventListener('change', () => {
        applyThemeState(toggle.checked);
        setTimeout(() => runRareEvent(Math.random() < 0.45), 920);
    });
});
