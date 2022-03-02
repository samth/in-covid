#lang racket

(require sawzall data-frame json graphite racket/runtime-path net/http-easy)
(require pict)
(require (only-in plot date-ticks) gregor threading)

(define-runtime-path here ".")
(define fips-codes (delay (~> (df-read/csv (build-path here "state_and_county_fips_master.csv"))
                              (where (state) (equal? state "IN")))))

(define-runtime-path json-path "covid-19-indiana-universal-report-current-public.json")

(define (update-data!)
  (define r (get "https://www.coronavirus.in.gov/map/covid-19-indiana-universal-report-current-public.json" #:stream? #t))
  (define p (response-output r))
  (define f (open-output-file json-path #:exists 'truncate))
  (define bs (port->bytes p))
  (write-bytes bs f))

;; this file is at https://www.coronavirus.in.gov/map/covid-19-indiana-universal-report-current-public.json
(define v (delay (read-json (open-input-file json-path))))

(define (hash-ref* v . rest)
  (for/fold ([v (force v)])
            ([k (in-list rest)])
    (hash-ref v k)))

(define data (delay (hash-ref* v 'metrics 'data)))

(define iso8601->posix (compose ->posix iso8601->date))
(define-syntax-rule (extract [fields keys] ...)
  (for/data-frame (fields ...) ([fields (hash-ref* data 'keys)] ...) (values fields ...)))

(define df
  (delay
    (extract [date date]
             [district district]
             [icu-covid m2b_hospitalized_icu_occupied_covid]
             [icu-not-covid m2b_hospitalized_icu_occupied_non_covid]
             [icu-supply m2b_hospitalized_icu_supply]
             [icu-available m2b_hospitalized_icu_available]
             [hospital-beds m1a_beds_all_occupied_beds_covid_19_smoothed]
             [hospital-pts m1a_beds_all_occupied_beds_covid_19_pts_smoothed]
             [hospital-pui m1a_beds_all_occupied_beds_covid_19_pui_smoothed]
             [admissions m1c_covid_cases_admission_rolling_sum]
             [cases m1e_covid_cases]
             [deaths m1e_covid_deaths]
             [tests m1e_covid_tests]
             [tests-adm m1e_covid_tests_adm])))

(define (->district d*)
  (cond [(not d*) "0"]
        [(number? d*) (number->string d*)]
        [(string->number d*) d*]
        [else (number->string (lookup-county-code d*))]))
  

(define (do-graph d* fields
                  #:margin [margin 20] #:legend [legend 'top-right] #:title [title "~a"] #:adjust [f values]
                  #:y-transform [yt #f])
  (define d (->district d*))
  (define p (apply
             graph
             #:data (f (~> (force df)
                           (where (district) (and (equal? district d)))))
             #:x-transform (only-ticks (date-ticks))
             #:x-conv iso8601->posix
             #:mapping (aes #:x "date")
             #:title
             (format title (district->name d))
             #:width 800
             #:legend-anchor legend
             #:x-label "Date"
             #:y-label ""
             #:y-transform yt
             (for/list ([(field* i) (in-indexed fields)])
               (define-values (field label)
                 (match field*
                   [(list f l) (values f l)]
                   [_ (values field* #f)]))
               (lines #:color (+ 1 i) #:mapping (aes #:y field) #:label label))))
  (add-margin margin p))

(define (icu-district [d* #f])
  (do-graph d*
            #:title "ICU occupancy, ~a"
            #:adjust (λ (df)
                       (~> df
                           (where (icu-supply) (>= icu-supply 1))
                           (create [total-icu (icu-covid icu-not-covid) (+ icu-covid icu-not-covid)])))
            '(("icu-covid"  "COVID ICU Patients")
              ("total-icu"  "Total ICU Patients")
              ("icu-supply" "Total ICU Beds"))))

(define (district->name d)
  (define d* (string->number d))
  (cond [(= 0 d*) "Indiana statewide"]
        [(< d* 20) (format "District ~a" d)]
        [else (lookup-county-name d*)]))

(define (hospital-district [d* #f])
  (do-graph d*
            #:title "Hospitalization, ~a"
            #:legend 'top-left
            #:adjust (λ (df) (~> df (where (hospital-beds) (> hospital-beds 0))))
            (list (list "hospital-beds" "All COVID Hospitalizations")
                  (list "hospital-pui" "COVID PUI Hospitalizations"))))

(define (admissions-district [d* #f])
  (do-graph d*
            #:legend 'top-left
            #:adjust (λ (df) (~> df
                                 (create [admissions-avg-7 ([admissions : vector]) (moving-average admissions)])))
            #:title "Hospital Admissions, ~a" (list  (list "admissions-avg-7" "7-day moving average"))))
(require math/statistics)
(define (moving-average v [k 7])
  (for/vector #:length (vector-length v)
    ([i (in-range (vector-length v))])
    (mean (vector-copy v (max 0 (- i k)) i))))

(define (cases-district [d* #f] #:log? [log? #f])
  (do-graph d*
            #:title "COVID Cases, ~a" (list (list "cases-avg-7" "7-day moving average")
                                            #;(list "cases" "Daily Cases")
                                            #;(list "cases-avg-14" "14-day moving average"))
            #:legend 'top-left
            #:y-transform (if log? logarithmic-transform #f)
            #:adjust (λ (df) (~> df
                                 (create [cases-avg-7 ([cases : vector]) (moving-average cases)]
                                         [cases-avg-14 ([cases : vector]) (moving-average cases 14)])
                                 (create [cases-avg-7 (cases-avg-7) (max cases-avg-7 1)])))))

(define (lookup-county-code pat)
  (for/first ([(f n) (in-data-frame (force fips-codes) "fips" "name")]
              #:when (regexp-match pat n))
    f))

(define (lookup-county-name n)
  (for/first ([(f name) (in-data-frame (force fips-codes) "fips" "name")]
              #:when (= n f))
    name))


(define (add-margin n p)
  (define bg (filled-rectangle #:color "white" #:draw-border? #f (+ n (pict-width p)) (+ n (pict-height p))))
  (cc-superimpose bg p))

(require simple-xlsx)

(define (write-fips-xlsx path)
  (let ([xlsx (new xlsx%)])
    (send xlsx add-data-sheet
          #:sheet_name "FIPS Codes"
          #:sheet_data
          (cons '("fips" "name" "state")
                (for/list ([(f name state) (in-data-frame (force fips-codes) "fips" "name" "state")]) (list f name state))))
    (write-xlsx-file xlsx path)))
; use as 
;(write-fips-xlsx "/home/samth/out.xlsx")
