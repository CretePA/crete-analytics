import CyclingHello from '@/components/CyclingHello';
import GenieChat from '@/components/GenieChat';

export default function HomePage() {
  return (
    <div className="page-stack">
      <div className="hello-card"><CyclingHello /></div>
      <GenieChat />
    </div>
  );
}
