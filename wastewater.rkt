#lang racket
(require fancy-app)
(require net/http-easy racket/runtime-path json sawzall graphite data-frame threading)
(require (only-in plot date-ticks make-axis-transform invertible-function log-ticks
                  plot-x-tick-label-angle plot-x-tick-label-anchor) gregor)
(define-runtime-path here ".")
(define u "https://bloomington.data.socrata.com/api/views/yv82-z42g/rows.csv?accessType=DOWNLOAD")


(define-runtime-path csv-path "rows.csv")


(define (update-data!)
  (define r (get u #:stream? #t))
  (define p (response-output r))
  (define f (open-output-file csv-path #:exists 'truncate))
  (define bs (port->bytes p))
  (write-bytes bs f))

(define date->posix (compose ->posix (λ (d) (parse-date d "MM/dd/yyyy' 12:00:00 AM'"))))
(define d (~> (df-read/csv csv-path)
              (where (location) (member location '("Blucher Poole" "Dillman")))))
(define (log* f)
  (if (or (= f 0) (< (log f) 0)) 0 (log f)))
(define safe-log-transform (transform (make-axis-transform (invertible-function log* exp)) (log-ticks #:scientific? #f)))
(define (go [start #f])

  (plot-x-tick-label-angle 90)
  (plot-x-tick-label-anchor 'top-right)
  (graph #:data d
         #:x-transform (only-ticks (date-ticks))
         #:x-conv date->posix
         #:x-min (date->posix (if start (string-append start " 12:00:00 AM") "09/30/2021 12:00:00 AM"))
         #:y-transform safe-log-transform
         #:x-label ""
         #:y-label "Gene Copies (log scale)"
         #:width 800
         #:mapping (aes #:x "date" #:y "gene_copies_per_100_ml" #:facet "location")
         (lines)
         (points)))

(module+ main
  (require racket/cmdline)
  (define d #f)
  (command-line #:once-each
                ["--update" "update the data" (begin (update-data!) (printf "Wastewater data updated.\n") (exit 0))]
                ["--start" date "set start date DD/MM/YYY" (set! d date)]
                #:args (path)
                (save-pict (go d) path)))
