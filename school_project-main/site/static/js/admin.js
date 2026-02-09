document.addEventListener('DOMContentLoaded', () => {
    const output = document.getElementById('console-output');
    if (output) {
        output.scrollTop = output.scrollHeight;
    }

    const input = document.getElementById('user-input');
    const buttons = document.querySelectorAll('.cmd-template');
    const specsNode = document.getElementById('console-command-specs');
    const select = document.getElementById('cmd-select');
    const argsWrap = document.getElementById('cmd-args');
    const preview = document.getElementById('cmd-preview');
    const applyBtn = document.getElementById('cmd-apply');
    const runBtn = document.getElementById('cmd-run-preview');

    let specs = {};
    if (specsNode && specsNode.textContent) {
        try {
            specs = JSON.parse(specsNode.textContent);
        } catch (e) {
            specs = {};
        }
    }

    const buildPreview = () => {
        if (!select) return '';
        const command = select.value || '';
        const spec = specs[command] || { args: [] };
        const inputs = argsWrap ? Array.from(argsWrap.querySelectorAll('input')) : [];
        const args = inputs.map((el) => el.value.trim()).filter(Boolean);
        const previewText = [command].concat(args).join(' ').trim();
        if (preview) preview.textContent = previewText || command;
        return previewText;
    };

    const rebuildArgs = () => {
        if (!select || !argsWrap) return;
        const command = select.value || '';
        const spec = specs[command] || { args: [] };
        argsWrap.innerHTML = '';
        (spec.args || []).forEach((arg) => {
            const field = document.createElement('div');
            field.className = 'field';
            const label = document.createElement('label');
            label.textContent = arg;
            const inputField = document.createElement('input');
            inputField.type = 'text';
            inputField.className = 'input-styled';
            inputField.placeholder = arg;
            inputField.addEventListener('input', buildPreview);
            field.appendChild(label);
            field.appendChild(inputField);
            argsWrap.appendChild(field);
        });
        buildPreview();
    };

    if (select) {
        select.addEventListener('change', rebuildArgs);
        rebuildArgs();
    }

    if (applyBtn) {
        applyBtn.addEventListener('click', () => {
            const previewText = buildPreview();
            if (input) {
                input.value = previewText;
                input.focus();
            }
        });
    }

    if (runBtn) {
        runBtn.addEventListener('click', () => {
            const previewText = buildPreview();
            if (!previewText) return;
            if (input) {
                input.value = previewText;
            }
            const form = input ? input.closest('form') : null;
            if (form) {
                form.submit();
            }
        });
    }

    buttons.forEach((btn) => {
        btn.addEventListener('click', () => {
            if (!input) return;
            input.value = btn.dataset.command || '';
            input.focus();
        });
    });
});
