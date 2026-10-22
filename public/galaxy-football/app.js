const video = document.querySelector('#game-video');
const playButton = document.querySelector('.video-start');
playButton.addEventListener('click', async () => {
  try { await video.play(); } catch { playButton.hidden = false; }
});
video.addEventListener('play', () => { playButton.hidden = true; });
video.addEventListener('ended', () => { playButton.hidden = false; });
