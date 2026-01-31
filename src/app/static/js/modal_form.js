// JavaScript for disabling form submissions if there are invalid fields
(function () {
    'use strict'

    // Fetch all the forms we want to apply custom Bootstrap validation styles to
    var forms = document.querySelectorAll('.needs-validation')

    // Loop over them and prevent submission
    Array.prototype.slice.call(forms)
        .forEach(function (form) {
            form.addEventListener('submit', function (event) {
                if (!form.checkValidity()) {
                    event.preventDefault()
                    event.stopPropagation()
                }

                form.classList.add('was-validated')
            }, false)
        })
})()

var betModal = document.getElementById('place_bet_modal')

// Set up the status change listener once (outside the modal show event)
// to avoid adding duplicate listeners every time the modal opens
var formStatus = document.getElementById('form-bet-status')
var formAmount = document.getElementById('form-bet-amount')
var formPrice = document.getElementById('form-bet-price')
var formProfitLoss = document.getElementById('form-bet-profitloss')

formStatus.addEventListener('change', function() {
    var status = formStatus.value
    var amount = parseFloat(formAmount.value) || 0
    var price = parseInt(formPrice.value) || -110

    if (status === 'Win' && amount > 0) {
        // Calculate winnings based on American odds
        var profit
        if (price > 0) {
            // Underdog: bet $100 to win $price
            profit = amount * (price / 100)
        } else {
            // Favorite: bet $|price| to win $100
            profit = amount * (100 / Math.abs(price))
        }
        formProfitLoss.value = profit.toFixed(2)
    } else if (status === 'Loss') {
        formProfitLoss.value = (-amount).toFixed(2)
    } else if (status === 'Push') {
        formProfitLoss.value = '0'
    } else if (status === 'Active') {
        formProfitLoss.value = '0'
    }
})

betModal.addEventListener('show.bs.modal', function (event) {
    // Button that triggered the modal
    var button = event.relatedTarget

    // Extract info from data-bs-* attributes
    var gameId = button.getAttribute('data-bs-gameId')
    var betDatetime = button.getAttribute('data-bs-betDatetime')
    var betLocation = button.getAttribute('data-bs-betLocation')
    var betLine = button.getAttribute('data-bs-betLine')
    var betAmount = button.getAttribute('data-bs-betAmount')
    var betDirection = button.getAttribute('data-bs-betDirection')
    var betPrice = button.getAttribute('data-bs-betPrice')
    var betStatus = button.getAttribute('data-bs-betStatus')
    var betProfitLoss = button.getAttribute('data-bs-betProfitLoss')
    // Note: accountBalance removed - now fetched server-side for security
    var spread = button.getAttribute('data-bs-spread')
    var pick = button.getAttribute('data-bs-pick')

    // Update the modal's "Current Bet Details" display
    var modalTitle = betModal.querySelector('#modal-title-js')
    var modalDatetime = betModal.querySelector('#bet-datetime-js')
    var modalLocation = betModal.querySelector('#bet-location-js')
    var modalLine = betModal.querySelector('#bet-line-js')
    var modalAmount = betModal.querySelector('#bet-amount-js')
    var modalDirection = betModal.querySelector('#bet-direction-js')
    var modalPrice = betModal.querySelector('#bet-price-js')
    var modalStatus = betModal.querySelector('#bet-status-js')
    var modalProfitLoss = betModal.querySelector('#bet-profitloss-js')

    modalTitle.textContent = 'Betting Details for ' + gameId
    modalDatetime.textContent = betDatetime
    modalLocation.textContent = betLocation
    modalLine.textContent = betLine
    modalAmount.textContent = betAmount
    modalDirection.textContent = betDirection
    modalPrice.textContent = betPrice
    modalStatus.textContent = betStatus
    modalProfitLoss.textContent = betProfitLoss

    // Update hidden form fields (only game ID - balance fetched server-side)
    var formGameID = betModal.querySelector('#form-gameID-js')
    formGameID.value = gameId

    // Pre-fill the form with existing bet data or prediction defaults
    var formLine = betModal.querySelector('#form-bet-line')
    var formDirection = betModal.querySelector('#form-bet-direction')
    var formLocation = betModal.querySelector('#form-bet-location')

    // If there's an existing bet, use those values
    if (betStatus && betStatus !== '-') {
        formStatus.value = betStatus
        formAmount.value = betAmount ? betAmount.replace('$', '') : ''
        formLine.value = betLine && betLine !== '-' ? betLine : ''
        formDirection.value = betDirection && betDirection !== '-' ? betDirection : 'Home'
        formPrice.value = betPrice && betPrice !== '-' ? betPrice : '-110'
        formProfitLoss.value = betProfitLoss ? betProfitLoss.replace('$', '') : '0'

        // Try to match location
        if (betLocation && betLocation !== '-') {
            var locationLower = betLocation.toLowerCase()
            if (locationLower.includes('draft')) formLocation.value = 'DraftKings'
            else if (locationLower.includes('fan')) formLocation.value = 'FanDuel'
            else if (locationLower.includes('mgm')) formLocation.value = 'MGM'
            else formLocation.value = 'Other'
        }
    } else {
        // New bet - use prediction defaults
        formStatus.value = 'Active'
        formAmount.value = ''
        formLine.value = spread && spread !== '-' ? spread : ''
        formDirection.value = pick && pick !== '-' ? pick : 'Home'
        formPrice.value = '-110'
        formLocation.value = 'DraftKings'
        formProfitLoss.value = '0'
    }
})
