import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import manifiestos from './src/routes/manifiestos.js';

const app = express();
app.use(cors({
  origin: ['https://front-aduanas.vercel.app', 'https://aduanas.stratego.pe', 'http://localhost:3000'],
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  credentials: true
}));
app.use(express.json());

app.use('/api/manifiestos', manifiestos);

app.get('/health', (_req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => console.log(`Servidor en http://localhost:${PORT}`));
