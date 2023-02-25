// Example starter JavaScript for disabling form submissions if there are invalid fields
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

// $('#place_bet_modal').on('hidden.bs.modal', function () {
//     location.reload();
// })

var betModal = document.getElementById('place_bet_modal')
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
    var bankBalance = button.getAttribute('data-bs-bankBalance')

    // If necessary, you could initiate an AJAX request here
    // and then do the updating in a callback.
    //
    // Update the modal's content.
    var formGameID = betModal.querySelector('#form-gameID-js')
    var oldProfitLoss = betModal.querySelector('#form-oldPF-js')
    var modalTitle = betModal.querySelector('#modal-title-js')
    var modalDatetime = betModal.querySelector('#bet-datetime-js')
    var modalLocation = betModal.querySelector('#bet-location-js')
    var modalLine = betModal.querySelector('#bet-line-js')
    var modalAmount = betModal.querySelector('#bet-amount-js')
    var modalDirection = betModal.querySelector('#bet-direction-js')
    var modalPrice = betModal.querySelector('#bet-price-js')
    var modalStatus = betModal.querySelector('#bet-status-js')
    var modalProfitLoss = betModal.querySelector('#bet-profitloss-js')
    var modalBankBalance = betModal.querySelector('#form-bankBalance-js')


    modalTitle.textContent = 'Betting Details for ' + gameId
    modalDatetime.textContent = betDatetime
    modalLocation.textContent = betLocation
    modalLine.textContent = betLine
    modalAmount.textContent = betAmount
    modalDirection.textContent = betDirection
    modalPrice.textContent = betPrice
    modalStatus.textContent = betStatus
    modalProfitLoss.textContent = betProfitLoss
    formGameID.value = gameId
    oldProfitLoss.value = betProfitLoss
    modalBankBalance.value = bankBalance
})
