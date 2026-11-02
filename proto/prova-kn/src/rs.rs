//! Codice Reed-Solomon sistematico (le prime k fette sono i dati così come sono, le altre m sono di parità), dalla
//! libreria `reed-solomon-simd`. Una scatola di S byte diventa un solo oggetto S3 di n fette uguali, una dopo l'altra:
//! la fetta i sta nei byte [i·f, (i+1)·f). Così il numero di oggetti e di PUT non cambia; cambia solo la dimensione
//! (n/k volte) e il numero di GET per lettura (n invece di 1).

use anyhow::Result;

/// Dimensione di una fetta: ceil(S/k) arrotondata al pari (la libreria vuole fette di lunghezza pari).
pub fn fetta(s: usize, k: usize) -> usize {
    let f = s.div_ceil(k);
    f + (f & 1)
}

/// Codifica: restituisce l'oggetto intero (k fette di dati con il riempimento a zero, poi m fette di parità).
pub fn codifica(dati: &[u8], k: usize, m: usize) -> Result<Vec<u8>> {
    let f = fetta(dati.len(), k);
    let mut buf = vec![0u8; f * k];
    buf[..dati.len()].copy_from_slice(dati);
    let fette: Vec<&[u8]> = buf.chunks(f).collect();
    let par = reed_solomon_simd::encode(k, m, fette)?;
    let mut out = buf;
    for p in par {
        out.extend_from_slice(&p);
    }
    Ok(out)
}

/// Ricostruisce i dati da k fette qualsiasi: `arrivate` = (indice della fetta, byte). Se sono le k fette di dati non
/// c'è niente da calcolare: si mettono in fila. Restituisce (dati, true se è servita la decodifica).
pub fn ricostruisci(arrivate: &[(usize, bytes::Bytes)], k: usize, m: usize, s: usize) -> Result<(Vec<u8>, bool)> {
    let f = fetta(s, k);
    let mut dati: Vec<Option<&[u8]>> = vec![None; k];
    let mut parita = Vec::new();
    for (i, b) in arrivate {
        anyhow::ensure!(b.len() == f, "fetta {} lunga {} invece di {}", i, b.len(), f);
        if *i < k {
            dati[*i] = Some(b);
        } else {
            parita.push((*i - k, &b[..]));
        }
    }
    let mancanti: Vec<usize> = (0..k).filter(|i| dati[*i].is_none()).collect();
    if mancanti.is_empty() {
        let mut out = Vec::with_capacity(f * k);
        for d in dati {
            out.extend_from_slice(d.unwrap());
        }
        out.truncate(s);
        return Ok((out, false));
    }
    anyhow::ensure!(parita.len() >= mancanti.len(), "fette insufficienti");
    let originali: Vec<(usize, &[u8])> = dati.iter().enumerate().filter_map(|(i, d)| d.map(|d| (i, d))).collect();
    let ricostruite = reed_solomon_simd::decode(k, m, originali, parita)?;
    let mut out = Vec::with_capacity(f * k);
    for i in 0..k {
        match dati[i] {
            Some(d) => out.extend_from_slice(d),
            None => out.extend_from_slice(&ricostruite[&i]),
        }
    }
    out.truncate(s);
    Ok((out, true))
}

#[cfg(test)]
mod t {
    use super::*;
    #[test]
    fn andata_e_ritorno() {
        for &(k, m) in &[(4usize, 1usize), (8, 2)] {
            for &s in &[4096usize, 65536, 1 << 20, 1000, 12345] {
                let dati: Vec<u8> = (0..s).map(|i| (i * 31 % 251) as u8).collect();
                let obj = codifica(&dati, k, m).unwrap();
                let f = fetta(s, k);
                assert_eq!(obj.len(), f * (k + m));
                // tutte le scelte di k fette su n
                let n = k + m;
                for mask in 0u32..(1 << n) {
                    if mask.count_ones() as usize != k {
                        continue;
                    }
                    let arr: Vec<(usize, bytes::Bytes)> = (0..n).filter(|i| mask & (1 << i) != 0).map(|i| (i, bytes::Bytes::copy_from_slice(&obj[i * f..(i + 1) * f]))).collect();
                    let (d, _) = ricostruisci(&arr, k, m, s).unwrap();
                    assert_eq!(d, dati);
                }
            }
        }
    }
}
