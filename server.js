import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import manifiestos from './src/routes/manifiestos.js';

const app = express();
app.use(cors());
app.use(express.json());

app.use('/api/manifiestos', manifiestos);

app.get('/health', (_req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => console.log(`Servidor en http://localhost:${PORT}`));
