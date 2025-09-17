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

    const specificImageUrl = `/images/${imageName}.jpg`;

    // Verificar si la imagen existe (usando una técnica de precarga)
    return checkImageExists(specificImageUrl)
        ? specificImageUrl
        : getRandomDefaultImage(trackId);
};

export const getRandomDefaultImage = (seed) => {
    // Número de imágenes por defecto disponibles
    const defaultImageCount = 5;

    // Usar el ID de la canción como semilla para consistencia, o generar aleatorio
    const randomIndex = seed
        ? (parseInt(seed) % defaultImageCount) + 1
        : Math.floor(Math.random() * defaultImageCount) + 1;

    return `/images/default/default${randomIndex}.jpg`;
};

export const checkImageExists = (url) => {
    // Esta es una verificación básica por ahora
    const knownImages = [
        'bohemian_rhapsody',
    ];

    const imageName = url.split('/').pop().replace('.jpg', '');
    return knownImages.includes(imageName);
};