document.addEventListener('DOMContentLoaded', () => {
    const body = document.body;
    const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') || '';

    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    if (csrfToken) {
        document.querySelectorAll('form').forEach((form) => {
            const method = (form.getAttribute('method') || 'GET').toUpperCase();
            if (method !== 'POST') return;
            let input = form.querySelector('input[name="csrf_token"]');
            if (!input) {
                input = document.createElement('input');
                input.type = 'hidden';
                input.name = 'csrf_token';
                form.prepend(input);
            }
            input.value = csrfToken;
        });
    }

    if (!prefersReducedMotion) {
        document.querySelectorAll('[class*="anim-delay-"]').forEach((el) => {
            const match = el.className.match(/anim-delay-(\d+)/);
            if (!match) return;
            const delay = Number(match[1]) * 0.2;
            el.style.animationDelay = `${delay}s`;
        });

        const autoFadeTargets = document.querySelectorAll('main.content > section, main.content > form, main.content > article, main.content > div');
        autoFadeTargets.forEach((el, idx) => {
            if (el.classList.contains('anim-fade-in')) return;
            const step = (idx % 6) + 1;
            el.classList.add('anim-fade-in', `anim-delay-${step}`);
            const jitter = (idx % 5) * 0.035;
            el.style.animationDelay = `${(step * 0.2) + jitter}s`;
        });

        const revealSelectors = [
            'main.content > *',
            '.panel',
            '.glass-card',
            '.dish-card',
            '.stats-card',
            '.thread-item',
            '.review-item',
            '.notify-item',
            '.flash-item',
            '.hero-chip',
            '.table-wrap tbody tr',
            '.footer-row .glass-card',
            '.field',
            '.field-grid > *',
            '.notify-filter > *'
        ];
        const revealNodes = Array.from(new Set(revealSelectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)))));
        revealNodes.forEach((node, idx) => {
            node.classList.add('anim-pop');
            node.style.setProperty('--stagger-index', `${idx % 24}`);
        });

        if ('IntersectionObserver' in window) {
            const observer = new IntersectionObserver((entries, obs) => {
                entries.forEach((entry) => {
                    if (!entry.isIntersecting) return;
                    const node = entry.target;
                    const step = Number(node.style.getPropertyValue('--stagger-index') || '0');
                    const delay = Math.min(500, step * 35);
                    setTimeout(() => node.classList.add('is-visible'), delay);
                    obs.unobserve(node);
                });
            }, { threshold: 0.12, rootMargin: '0px 0px -8% 0px' });
            revealNodes.forEach((node) => observer.observe(node));
        } else {
            revealNodes.forEach((node) => node.classList.add('is-visible'));
        }
    } else {
        const allRevealElements = document.querySelectorAll('[class*="anim-"], main.content > *, .panel, .glass-card, .dish-card, .stats-card, .thread-item, .review-item, .notify-item, .flash-item, .hero-chip, .table-wrap tbody tr, .footer-row .glass-card, .field, .field-grid > *, .notify-filter > *');
        allRevealElements.forEach((el) => {
            el.classList.remove('anim-fade-in', 'anim-pop');
            el.style.animationDelay = '0s';
            el.classList.add('is-visible');
        });
    }

    const interactiveNodes = document.querySelectorAll(
        '.button-custom, .panel, .form-card, .dish-card, .stats-card, .glass-card, .flash-item, .notify-item, .thread-item, .review-item, .input-styled, .avatar-container, .hero-chip, .message-panel, .table-wrap tbody tr, .theme-switch, .UserMenuListDropdown a, .dish-card-link'
    );
    const hoverTimers = new WeakMap();
    const pressTimers = new WeakMap();
    const clearTimer = (bucket, node) => {
        const timer = bucket.get(node);
        if (timer) {
            clearTimeout(timer);
            bucket.delete(node);
        }
    };
    const scheduleClassDrop = (bucket, node, className, delay) => {
        clearTimer(bucket, node);
        const timer = setTimeout(() => {
            node.classList.remove(className);
            bucket.delete(node);
        }, delay);
        bucket.set(node, timer);
    };

    interactiveNodes.forEach((node) => {
        node.classList.add('interactive-node');
        node.addEventListener('mouseenter', () => {
            if (!prefersReducedMotion) {
                clearTimer(hoverTimers, node);
                clearTimer(pressTimers, node);
                node.classList.remove('is-hover-out');
                node.classList.remove('is-press-out');
                node.classList.add('is-hover-anim');
            }
        });
        node.addEventListener('mouseleave', () => {
            if (!prefersReducedMotion) {
                node.classList.remove('is-hover-anim');
                node.classList.add('is-hover-out');
                scheduleClassDrop(hoverTimers, node, 'is-hover-out', 980);
                node.classList.remove('is-press-anim');
                node.classList.add('is-press-out');
                scheduleClassDrop(pressTimers, node, 'is-press-out', 760);
            }
        });
        node.addEventListener('mousedown', () => {
            clearTimer(pressTimers, node);
            node.classList.remove('is-press-out');
            if (!prefersReducedMotion) {
                node.classList.add('is-press-anim');
            }
        });
        node.addEventListener('mouseup', () => {
            if (!prefersReducedMotion) {
                node.classList.remove('is-press-anim');
                node.classList.add('is-press-out');
                scheduleClassDrop(pressTimers, node, 'is-press-out', 760);
                if (node.matches(':hover')) {
                    node.classList.add('is-hover-anim');
                } else {
                    node.classList.remove('is-hover-anim');
                    node.classList.add('is-hover-out');
                    scheduleClassDrop(hoverTimers, node, 'is-hover-out', 980);
                }
            }
        });
        node.addEventListener('touchstart', () => {
            clearTimer(pressTimers, node);
            node.classList.remove('is-press-out');
            if (!prefersReducedMotion) {
                node.classList.add('is-press-anim');
            }
        }, { passive: true });
        node.addEventListener('touchend', () => {
            if (!prefersReducedMotion) {
                node.classList.remove('is-press-anim');
                node.classList.add('is-press-out');
                scheduleClassDrop(pressTimers, node, 'is-press-out', 760);
            }
        }, { passive: true });
        node.addEventListener('touchcancel', () => {
            if (!prefersReducedMotion) {
                node.classList.remove('is-press-anim');
                node.classList.remove('is-press-out');
            }
        }, { passive: true });
    });

    const buttons = document.querySelectorAll('.button-custom');
    buttons.forEach((button) => {
        button.addEventListener('click', (event) => {
            if (prefersReducedMotion) return;
            const rect = button.getBoundingClientRect();
            const size = Math.max(rect.width, rect.height) * 1.4;
            const ripple = document.createElement('span');
            ripple.className = 'press-ripple';
            ripple.style.width = `${size}px`;
            ripple.style.height = `${size}px`;
            const clientX = typeof event.clientX === 'number' && event.clientX > 0 ? event.clientX : rect.left + (rect.width / 2);
            const clientY = typeof event.clientY === 'number' && event.clientY > 0 ? event.clientY : rect.top + (rect.height / 2);
            ripple.style.left = `${clientX - rect.left - (size / 2)}px`;
            ripple.style.top = `${clientY - rect.top - (size / 2)}px`;
            button.appendChild(ripple);
            requestAnimationFrame(() => ripple.classList.add('run'));
            setTimeout(() => ripple.remove(), 2200);
        });
    });

    const tiltNodes = document.querySelectorAll('.hero-panel, .panel, .glass-card, .dish-card, .stats-card, .form-card');
    tiltNodes.forEach((node) => {
        node.style.setProperty('--mx', '50%');
        node.style.setProperty('--my', '50%');
        node.style.setProperty('--tilt-x', '0deg');
        node.style.setProperty('--tilt-y', '0deg');
        node.addEventListener('pointermove', (event) => {
            if (prefersReducedMotion) return;
            const rect = node.getBoundingClientRect();
            if (!rect.width || !rect.height) return;
            const px = (event.clientX - rect.left) / rect.width;
            const py = (event.clientY - rect.top) / rect.height;
            const safeX = Math.min(1, Math.max(0, px));
            const safeY = Math.min(1, Math.max(0, py));
            const tiltX = ((0.5 - safeY) * 3.2).toFixed(2);
            const tiltY = ((safeX - 0.5) * 3.6).toFixed(2);
            node.style.setProperty('--mx', `${(safeX * 100).toFixed(2)}%`);
            node.style.setProperty('--my', `${(safeY * 100).toFixed(2)}%`);
            node.style.setProperty('--tilt-x', `${tiltX}deg`);
            node.style.setProperty('--tilt-y', `${tiltY}deg`);
        });
        node.addEventListener('pointerleave', () => {
            node.style.setProperty('--mx', '50%');
            node.style.setProperty('--my', '50%');
            node.style.setProperty('--tilt-x', '0deg');
            node.style.setProperty('--tilt-y', '0deg');
        });
    });

    requestAnimationFrame(() => body.classList.add('motion-ready'));
});
