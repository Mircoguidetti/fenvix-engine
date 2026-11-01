//! micro-efs: il programma della micro-prova del quaderno a turno su Amazon EFS (S12).
//!
//! Comandi:
//!   scrivi          un banco di latenza (una combinazione)
//!   matrice         tutte le combinazioni, con pause tra le fasi
//!   gara            banco 9: gara per il turno (O_EXCL), da lanciare su ogni istanza
//!   gara-verifica   banco 9: esattamente un vincitore per round
//!   presa-titolare  banco 10 / zombie: il titolare (istanza A)
//!   presa-subentra  banco 10 / zombie: chi prende il turno (istanza B)
//!   presa-verifica  banco 10 / zombie: nessun lotto confermato mancante
//!   leggi-coda      letture dal libro da un'altra istanza mentre uno scrive
//!   metriche        CloudWatch di EFS per ogni fase, e conto al mese
//!   presa-figlio    (interno) il processo che scrive davvero
//!
//! Nessuna chiamata ad AWS tranne `metriche`, che usa la AWS CLI.

mod coda;
mod comune;
mod gara;
mod metriche;
mod presa;
mod scrivi;

use comune::{fail, Args};

const AIUTO: &str = "uso: micro-efs <comando> [--opzioni]

  scrivi --dir D [--controllo-dir C] [--modo fsync|dsync] [--lotto-kib 4] [--file 1]
         [--durata-s 10 | --lotti N] [--controllo no|si|pipeline]
         [--controllo-modo open|stat|odirect] [--solo-controllo si|no] [--intervallo-ms 0]
         [--prealloca-mib 0] [--nome-libro X] [--fase nome] [--fasi fasi.jsonl]
  matrice --dir D [--controllo-dir C] [--lotti-kib 1,4,32,64,256] [--files 1,8,64]
         [--modi fsync,dsync] [--controlli no,si,pipeline] [--durata-s 10] [--pausa-s 90]
         [--anche-solo-controllo si] [--controllo-modi open,stat,odirect]
         [--fase prefisso] [--fasi fasi.jsonl]
  leggi-coda --libro D/coda/X-0.log [--durata-s 60] [--letture-s 500] [--odirect si|no]
         [--poll-ms 1] [--fase nome] [--fasi fasi.jsonl]
  gara --dir D --run R [--id ID] [--concorrenti 4] [--round 1000] [--periodo-ms 50]
       [--inizio-ms <unix ms uguale su tutte le istanze>]
  gara-verifica --dir D --run R [--round 1000]
  presa-titolare --dir D [--controllo-dir C] --run R [--cicli 20] [--lotti 100]
       [--modo uccidi|ferma|vivo] [--lotto-kib 4] [--modo-sync fsync|dsync] [--prealloca-mib 0]
  presa-subentra --dir D [--controllo-dir C] --run R [--cicli 20]
  presa-verifica --dir D --run R [--cicli 20]
  metriche --fs-id fs-... --fasi fasi.jsonl [--regione eu-west-1] [--aws aws]
       [--prezzo-scrittura-gb 0.07] [--prezzo-lettura-gb 0.03] [--prezzo-prov-mibs-mese 6.60]

--controllo-dir: lo stesso file system montato una seconda volta con
lookupcache=none,noac,nosharecache. Lì si fanno le domande \"esiste l'epoca nuova?\".";

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.len() < 2 || argv[1] == "--help" || argv[1] == "aiuto" {
        println!("{AIUTO}");
        return;
    }
    let a = Args::parse(&argv[2..]).unwrap_or_else(|e| fail(&e));
    match argv[1].as_str() {
        "scrivi" => println!("{}", scrivi::esegui(&scrivi::da_args(&a)).riga),
        "matrice" => scrivi::matrice(&a),
        "gara" => gara::gara(&a),
        "gara-verifica" => {
            if !gara::verifica(&a) {
                std::process::exit(1)
            }
        }
        "presa-titolare" => presa::titolare(&a),
        "presa-subentra" => presa::subentra(&a),
        "presa-verifica" => {
            if !presa::verifica(&a) {
                std::process::exit(1)
            }
        }
        "presa-figlio" => presa::figlio(&a),
        "metriche" => metriche::metriche(&a),
        "leggi-coda" => coda::leggi_coda(&a),
        c => fail(&format!("comando sconosciuto: {c}\n{AIUTO}")),
    }
}
