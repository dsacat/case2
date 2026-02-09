document.addEventListener('DOMContentLoaded', () => {
    const upload = document.getElementById('avatar-upload');
    if (!upload) return;
    const csrfToken = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') || '';

    upload.addEventListener('change', () => {
        const file = upload.files && upload.files[0];
        if (!file) return;

        const formData = new FormData();
        formData.append('file', file);

        fetch('/upload_avatar/', {
            method: 'POST',
            headers: csrfToken ? { 'X-CSRF-Token': csrfToken } : {},
            body: formData,
        })
            .then((response) => response.json())
            .then((data) => {
                if (data.status === 'success') {
                    window.location.reload();
                    return;
                }
                alert(data.message || 'Ошибка загрузки');
            })
            .catch(() => alert('Ошибка загрузки файла'));
    });
});
