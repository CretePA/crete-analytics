'use client';
import { useState, useEffect } from 'react';

const HELLO_PHRASES = [
  { text: 'Hello World', lang: 'English' },
  { text: 'Hola Mundo', lang: 'Spanish' },
  { text: 'Bonjour le Monde', lang: 'French' },
  { text: 'Hallo Welt', lang: 'German' },
  { text: 'Ciao Mondo', lang: 'Italian' },
  { text: 'Olá Mundo', lang: 'Portuguese' },
  { text: 'こんにちは世界', lang: 'Japanese' },
  { text: '你好世界', lang: 'Chinese' },
  { text: '안녕하세요 세계', lang: 'Korean' },
  { text: 'مرحبا بالعالم', lang: 'Arabic' },
  { text: 'Привет мир', lang: 'Russian' },
  { text: 'Hej Världen', lang: 'Swedish' },
  { text: 'Merhaba Dünya', lang: 'Turkish' },
  { text: 'Γειά σου Κόσμε', lang: 'Greek' },
  { text: 'नमस्ते दुनिया', lang: 'Hindi' },
  { text: 'Witaj Świecie', lang: 'Polish' },
  { text: 'Xin chào Thế giới', lang: 'Vietnamese' },
  { text: 'Hei Maailma', lang: 'Finnish' },
];

export default function CyclingHello() {
  const [index, setIndex] = useState(0);
  const [fade, setFade] = useState('in');

  useEffect(() => {
    const timer = setInterval(() => {
      setFade('out');
      setTimeout(() => { setIndex(i => (i + 1) % HELLO_PHRASES.length); setFade('in'); }, 600);
    }, 10000);
    return () => clearInterval(timer);
  }, []);

  const phrase = HELLO_PHRASES[index];
  return (
    <div className="hello-hero">
      <div className={`hello-text ${fade}`}><span className="hello-phrase">{phrase.text}</span></div>
      <div className={`hello-lang ${fade}`}>{phrase.lang}</div>
    </div>
  );
}
