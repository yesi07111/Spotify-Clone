export const getTrackImageUrl = (trackTitle, trackId) => {
    if (!trackTitle) return getRandomDefaultImage();

    // Crear un nombre de archivo válido a partir del título
    const imageName = trackTitle
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^\w-]+/g, '')
        .replace(/_+/g, '_')
        .replace(/-+/g, '_')
        .replace(/^[-_]+/, '')
        .replace(/[-_]+$/, '');

    // Primero intentar con ambos formatos
    const jpgUrl = `/images/${imageName}.jpg`;
    const pngUrl = `/images/${imageName}.png`;

    // Verificar si alguna de las imágenes existe
    if (checkImageExists(jpgUrl)) {
        return jpgUrl;
    } else if (checkImageExists(pngUrl)) {
        return pngUrl;
    } else {
        return getRandomDefaultImage(trackId);
    }
};

export const getRandomDefaultImage = (seed) => {
    const defaultImageCount = 3;
    const randomIndex = seed
        ? (parseInt(seed) % defaultImageCount) + 1
        : Math.floor(Math.random() * defaultImageCount) + 1;

    // Intentar con PNG primero, luego JPG
    const pngUrl = `/images/default/default${randomIndex}.png`;
    const jpgUrl = `/images/default/default${randomIndex}.jpg`;

    return checkImageExists(pngUrl) ? pngUrl : jpgUrl;
};

export const checkImageExists = (url) => {
    const knownImages = [
        'bohemian_rhapsody',
    ];

    const imageName = url.split('/').pop().replace('.jpg', '').replace('.png', '');
    return knownImages.includes(imageName);
};

// Función mejorada que maneja errores de carga de imágenes
export const loadImageWithFallback = (url, fallbackUrl, callback) => {
    const img = new Image();
    img.onload = () => callback(url);
    img.onerror = () => callback(fallbackUrl);
    img.src = url;
};