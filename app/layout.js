import './globals.css';
import NavLayout from '@/components/NavLayout';

export const metadata = {
  title: 'Crete Analytics',
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <NavLayout>{children}</NavLayout>
      </body>
    </html>
  );
}
